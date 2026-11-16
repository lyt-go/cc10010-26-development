"""数据回填业务规则：资源准入与排队结算。

结算口径（状态流转只允许在本模块内发生，路由层不做业务判断）：
- 归并结算：以回填记录上最新的分区积压量与已启用资源池的配额（最大并发数、
  作业白名单）为输入，统一结算后生成资源准入数据快照；
- 白名单匹配：作业名称落在「已启用」资源池的作业白名单内，才参与该池配额结算，
  否则拒绝准入，作业不能绕过白名单占用并发槽位；
- 多池匹配：同一作业匹配多个资源池时，已分配过的作业延续既存资源配置，
  否则选用剩余槽位最多的资源池（池编号升序兜底，结果可复现）；
- 最新积压：较晚到达的积压更新会改变优先级与并发占用——积压越大优先级越高，
  占用槽位 = ceil(分区积压量 / 500)，至少 1 个；
- 超限拒绝：分区积压量超出允许上限，或所需槽位超出资源池最大并发数，拒绝准入；
- 槽位争用：结算全程串行（加锁），最后一个槽位只分配给优先级最高的一条；
- 幂等：已准入的作业重复提交不再重复占用，只生效一次；
- 回写：准入结果回写运行实例清单，运行实例只按结算后的并发占用与分区积压量启停回填。
"""
from __future__ import annotations

import math
import threading
from datetime import datetime
from typing import Any

from app.store import store

MODULE = "backfill"
SNAPSHOT_MODULE = "backfillsnapshot"
RUNTIME_MODULE = "runtime"
RESOURCE_MODULE = "resource"

REQUIRED_FIELDS = ["回填编号", "作业名称", "目标数据表"]
STATUS_ORDER = ["待结算", "排队中", "已准入", "已拒绝", "已停止", "已完成"]
ACTION_RULES = {"停止回填": "已停止", "标记完成": "已完成"}
SETTLEABLE_STATUSES = {"待结算", "排队中", "已拒绝", "已停止"}

BACKLOG_SLOT_STEP = 500  # 每 500 分区积压折算 1 个并发槽
MAX_ALLOWED_BACKLOG = 2000  # 分区积压量允许上限，超出拒绝准入

# 准入结果 -> (运行实例状态, 运行实例执行状态)：白名单与并发结算共同决定回填状态
RUNTIME_STATE = {
    "已准入": ("运行中", "回填中"),
    "排队中": ("草稿", "排队待启"),
    "已拒绝": ("已终止", "准入被拒"),
    "已停止": ("已终止", "回填已停止"),
    "已完成": ("成功", "回填完成"),
}

_LOCK = threading.Lock()
_BATCH_SEQ = 0


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _required_slots(backlog: int) -> int:
    """分区积压量折算并发槽位：每 500 积压占 1 槽，至少 1 槽。"""
    return max(1, math.ceil(backlog / BACKLOG_SLOT_STEP))


def _next_batch_no() -> str:
    global _BATCH_SEQ
    _BATCH_SEQ += 1
    return f"SETL-{_BATCH_SEQ:04d}"


class BackfillService:
    # ---- 列表、明细与登记 ----
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        job: str | None = None,
        plane: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("回填编号", ""))]
        if job:
            rows = [row for row in rows if job in str(row.get("作业名称", ""))]
        if plane:
            rows = [row for row in rows if plane in str(row.get("业务面", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["业务面"] = str(values.get("业务面") or "默认业务面")
        entry["分区积压量"] = max(0, _to_int(values.get("分区积压量")))
        entry["积压更新时间"] = _now()
        entry["匹配资源池"] = ""
        entry["占用并发数"] = 0
        entry["结算批次号"] = ""
        entry["准入说明"] = ""
        entry["pending"] = True
        entry["abnormal"] = False
        self._set_status(entry, STATUS_ORDER[0])
        rows.append(entry)
        return entry, []

    # ---- 资源池视图：白名单匹配与槽位占用 ----
    def _enabled_pools(self) -> list[dict[str, Any]]:
        return [pool for pool in store.rows(RESOURCE_MODULE) if pool.get("status") == "已启用"]

    @staticmethod
    def _whitelist(pool: dict[str, Any]) -> list[str]:
        raw = str(pool.get("作业白名单") or "")
        for sep in ("，", "、", ";", "；"):
            raw = raw.replace(sep, ",")
        return [item.strip() for item in raw.split(",") if item.strip()]

    def _matched_pools(self, job_name: str) -> list[dict[str, Any]]:
        return [pool for pool in self._enabled_pools() if job_name in self._whitelist(pool)]

    def _pool_usage(self) -> dict[str, int]:
        """各资源池已占用并发 = 已准入回填记录的占用槽位之和（含各相邻业务面）。"""
        usage: dict[str, int] = {}
        for rec in store.rows(MODULE):
            if rec.get("status") == "已准入" and rec.get("匹配资源池"):
                pool_no = str(rec["匹配资源池"])
                usage[pool_no] = usage.get(pool_no, 0) + _to_int(rec.get("占用并发数"))
        return usage

    # ---- 状态流转 ----
    @staticmethod
    def _set_status(rec: dict[str, Any], status: str) -> None:
        rec["status"] = status
        rec["准入结果"] = status

    def _admit(self, rec: dict[str, Any], pool: dict[str, Any], need: int, usage: dict[str, int]) -> None:
        pool_no = str(pool.get("池编号"))
        self._set_status(rec, "已准入")
        rec["匹配资源池"] = pool_no
        rec["占用并发数"] = need
        rec["pending"] = False
        rec["abnormal"] = False
        rec["准入说明"] = f"准入资源池{pool.get('池名称')}，占用 {need} 个并发槽"
        usage[pool_no] = usage.get(pool_no, 0) + need

    def _queue(self, rec: dict[str, Any], pool: dict[str, Any], need: int, usage: dict[str, int]) -> None:
        pool_no = str(pool.get("池编号"))
        remaining = max(0, _to_int(pool.get("最大并发数")) - usage.get(pool_no, 0))
        self._set_status(rec, "排队中")
        rec["匹配资源池"] = pool_no
        rec["占用并发数"] = 0
        rec["pending"] = True
        rec["abnormal"] = False
        rec["准入说明"] = f"资源池{pool.get('池名称')}剩余槽位 {remaining} 不足（需 {need} 个），排队等待"

    def _reject(self, rec: dict[str, Any], reason: str) -> None:
        self._set_status(rec, "已拒绝")
        rec["匹配资源池"] = ""
        rec["占用并发数"] = 0
        rec["pending"] = False
        rec["abnormal"] = True
        rec["准入说明"] = reason

    @staticmethod
    def _result(rec: dict[str, Any] | None, code: str, result: str, note: str) -> dict[str, Any]:
        rec = rec or {}
        return {
            "回填编号": code,
            "作业名称": rec.get("作业名称", "—"),
            "准入结果": result,
            "匹配资源池": rec.get("匹配资源池") or "—",
            "占用并发数": _to_int(rec.get("占用并发数")),
            "分区积压量": _to_int(rec.get("分区积压量")),
            "说明": note,
        }

    # ---- 结算批次：多选记录一次提交，统一批量处理 ----
    def settle_batch(self, ids: list[int]) -> tuple[dict[str, Any], str]:
        with _LOCK:  # 结算全程串行：并发争用最后一个槽位只允许一项占用
            batch_no = _next_batch_no()
            results: list[dict[str, Any]] = []
            candidates: list[dict[str, Any]] = []
            for entry_id in dict.fromkeys(ids):  # 批次内去重，同一作业只生效一次
                rec = store.find(MODULE, entry_id)
                if rec is None:
                    results.append(self._result(None, f"#{entry_id}", "未生效", f"回填记录 {entry_id} 不存在或已归档"))
                    continue
                status = rec.get("status")
                if status == "已准入":
                    results.append(self._result(rec, str(rec.get("回填编号")), "未生效",
                                                "已处于准入状态，重复提交只生效一次，沿用既存资源配置"))
                elif status == "已完成":
                    results.append(self._result(rec, str(rec.get("回填编号")), "未生效", "回填已完成，不再参与结算"))
                elif status not in SETTLEABLE_STATUSES:
                    results.append(self._result(rec, str(rec.get("回填编号")), "未生效",
                                                f"当前状态「{status}」不参与结算"))
                else:
                    candidates.append(rec)
            # 最新分区积压量决定优先级：积压越大越先结算，编号升序兜底
            candidates.sort(key=lambda rec: (-_to_int(rec.get("分区积压量")), int(rec.get("id", 0))))
            self._settle_candidates(candidates, batch_no, results)
            snapshot = self._build_snapshot(batch_no, results)
            self._sync_runtime()
            summary = {
                "结算批次号": batch_no,
                "快照编号": snapshot["快照编号"],
                "准入": sum(1 for item in results if item["准入结果"] == "已准入"),
                "排队": sum(1 for item in results if item["准入结果"] == "排队中"),
                "拒绝": sum(1 for item in results if item["准入结果"] == "已拒绝"),
                "未生效": sum(1 for item in results if item["准入结果"] == "未生效"),
                "结果": results,
            }
            message = (
                f"结算批次 {batch_no} 完成：准入 {summary['准入']} 条、排队 {summary['排队']} 条、"
                f"拒绝 {summary['拒绝']} 条，已生成资源准入数据快照 {snapshot['快照编号']}"
            )
            return summary, message

    def _settle_candidates(self, candidates: list[dict[str, Any]], batch_no: str,
                           results: list[dict[str, Any]]) -> None:
        pools_by_no = {str(pool.get("池编号")): pool for pool in self._enabled_pools()}
        usage = self._pool_usage()
        for rec in candidates:
            rec["结算批次号"] = batch_no
            code = str(rec.get("回填编号"))
            backlog = _to_int(rec.get("分区积压量"))
            if backlog > MAX_ALLOWED_BACKLOG:
                self._reject(rec, f"分区积压量 {backlog} 超出允许上限 {MAX_ALLOWED_BACKLOG}，拒绝准入")
                results.append(self._result(rec, code, "已拒绝", rec["准入说明"]))
                continue
            need = _required_slots(backlog)
            pools = self._matched_pools(str(rec.get("作业名称", "")))
            if not pools:
                self._reject(rec, "作业不在任何已启用资源池的白名单内，拒绝准入")
                results.append(self._result(rec, code, "已拒绝", rec["准入说明"]))
                continue
            assigned = str(rec.get("匹配资源池") or "")
            if assigned and any(str(pool.get("池编号")) == assigned for pool in pools):
                # 延续既存资源配置：多池匹配冲突时只沿用原池配额
                self._settle_on_pool(rec, pools_by_no[assigned], need, usage, results)
                continue
            # 多池匹配：按剩余槽位从多到少选池，池编号升序兜底
            ordered = sorted(
                pools,
                key=lambda pool: (
                    -(_to_int(pool.get("最大并发数")) - usage.get(str(pool.get("池编号")), 0)),
                    str(pool.get("池编号")),
                ),
            )
            fitted = [pool for pool in ordered if need <= _to_int(pool.get("最大并发数"))]
            if not fitted:
                self._reject(rec, f"所需并发 {need} 个槽位超出所有匹配资源池的最大并发数，拒绝准入")
                results.append(self._result(rec, code, "已拒绝", rec["准入说明"]))
                continue
            admitted = False
            for pool in fitted:
                pool_no = str(pool.get("池编号"))
                if usage.get(pool_no, 0) + need <= _to_int(pool.get("最大并发数")):
                    self._admit(rec, pool, need, usage)
                    results.append(self._result(rec, code, "已准入", rec["准入说明"]))
                    admitted = True
                    break
            if not admitted:
                self._queue(rec, fitted[0], need, usage)
                results.append(self._result(rec, code, "排队中", rec["准入说明"]))

    def _settle_on_pool(self, rec: dict[str, Any], pool: dict[str, Any], need: int,
                        usage: dict[str, int], results: list[dict[str, Any]]) -> None:
        """在既存资源池上结算：槽位够则准入，暂时不足则排队，永远放不下则拒绝。"""
        code = str(rec.get("回填编号"))
        pool_no = str(pool.get("池编号"))
        max_concurrency = _to_int(pool.get("最大并发数"))
        if need > max_concurrency:
            self._reject(rec, f"所需并发 {need} 个槽位超出既存资源池{pool.get('池名称')}最大并发数 "
                              f"{max_concurrency}，拒绝准入")
        elif usage.get(pool_no, 0) + need <= max_concurrency:
            self._admit(rec, pool, need, usage)
        else:
            self._queue(rec, pool, need, usage)
        results.append(self._result(rec, code, str(rec.get("status")), rec["准入说明"]))

    # ---- 分区积压更新：较晚到达的积压会改变优先级与并发占用 ----
    def update_backlog(self, entry_id: int, backlog_value: Any) -> tuple[dict[str, Any] | None, str]:
        with _LOCK:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"回填记录 {entry_id} 不存在或已归档"
            backlog = _to_int(backlog_value, default=-1)
            if backlog < 0:
                return None, "分区积压量必须是不小于 0 的整数"
            entry["分区积压量"] = backlog
            entry["积压更新时间"] = _now()
            if entry.get("status") != "已准入":
                return entry, "分区积压量已更新，将在下次结算时按最新积压参与排队"
            # 已准入记录的积压更新会改变并发占用：释放原槽位后按最新积压重新结算，延续既存资源配置
            entry["status"] = "待结算"
            entry["占用并发数"] = 0
            batch_no = _next_batch_no()
            results: list[dict[str, Any]] = []
            self._settle_candidates([entry], batch_no, results)
            snapshot = self._build_snapshot(batch_no, results)
            self._sync_runtime()
            return entry, f"分区积压量已更新并按最新积压重新结算，资源准入数据快照 {snapshot['快照编号']} 已刷新"

    # ---- 启停动作：释放槽位并刷新快照 ----
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        with _LOCK:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"回填记录 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于数据回填可执行范围"
            if entry.get("status") != "已准入":
                return None, f"仅「已准入」的回填记录可执行{action}，当前状态「{entry.get('status')}」"
            target = ACTION_RULES[action]
            self._set_status(entry, target)
            entry["占用并发数"] = 0
            entry["pending"] = False
            entry["abnormal"] = False
            entry["准入说明"] = f"已{action}，并发槽位已释放"
            batch_no = _next_batch_no()
            entry["结算批次号"] = batch_no
            snapshot = self._build_snapshot(
                batch_no, [self._result(entry, str(entry.get("回填编号")), target, entry["准入说明"])]
            )
            self._sync_runtime()
            return entry, f"回填记录已{action}，槽位已释放并刷新资源准入数据快照 {snapshot['快照编号']}"

    # ---- 资源准入数据快照 ----
    def latest_snapshot(self) -> dict[str, Any] | None:
        rows = store.rows(SNAPSHOT_MODULE)
        return rows[-1] if rows else None

    def _build_snapshot(self, batch_no: str, results: list[dict[str, Any]]) -> dict[str, Any]:
        """生成资源准入数据快照：各已启用资源池的槽位占用 + 本批次准入结果。"""
        usage = self._pool_usage()
        admitted = [rec for rec in store.rows(MODULE) if rec.get("status") == "已准入"]
        pool_view = []
        for pool in self._enabled_pools():
            pool_no = str(pool.get("池编号"))
            used = usage.get(pool_no, 0)
            max_concurrency = _to_int(pool.get("最大并发数"))
            pool_view.append({
                "池编号": pool_no,
                "池名称": pool.get("池名称"),
                "最大并发数": max_concurrency,
                "已占用并发": used,
                "剩余槽位": max(0, max_concurrency - used),
                "占用明细": [
                    {
                        "回填编号": rec.get("回填编号"),
                        "作业名称": rec.get("作业名称"),
                        "业务面": rec.get("业务面"),
                        "占用并发数": _to_int(rec.get("占用并发数")),
                        "分区积压量": _to_int(rec.get("分区积压量")),
                    }
                    for rec in admitted if str(rec.get("匹配资源池")) == pool_no
                ],
            })
        rows = store.rows(SNAPSHOT_MODULE)
        snapshot = {
            "id": len(rows) + 1,
            "快照编号": f"SNAP-{len(rows) + 1:04d}",
            "结算批次号": batch_no,
            "结算时间": _now(),
            "资源池占用": pool_view,
            "准入结果": results,
            "status": "已生成",
            "pending": False,
            "abnormal": any(item.get("准入结果") == "已拒绝" for item in results),
        }
        rows.append(snapshot)
        return snapshot

    # ---- 回写运行实例清单：运行实例读取快照后启停回填 ----
    def _sync_runtime(self) -> None:
        runtime_rows = store.rows(RUNTIME_MODULE)
        for rec in store.rows(MODULE):
            batch_no = str(rec.get("结算批次号") or "")
            if not batch_no:
                continue
            row = next((item for item in runtime_rows if item.get("回填编号") == rec.get("回填编号")), None)
            if row is None:
                next_id = max((int(item.get("id", 0)) for item in runtime_rows), default=0) + 1
                row = {
                    "id": next_id,
                    "记录编号": f"RUNT-{next_id:04d}",
                    "作业名称": rec.get("作业名称"),
                    "开始时间": _today(),
                    "结束时间": "",
                    "读取行数": "-",
                    "写入行数": "-",
                    "错误信息": "",
                    "回填编号": rec.get("回填编号"),
                }
                runtime_rows.append(row)
            run_status, run_state = RUNTIME_STATE.get(str(rec.get("status")), ("草稿", "待结算"))
            row["status"] = run_status
            row["执行状态"] = run_state
            row["匹配资源池"] = rec.get("匹配资源池") or "—"
            row["占用并发数"] = _to_int(rec.get("占用并发数"))
            row["分区积压量"] = _to_int(rec.get("分区积压量"))
            row["结算批次号"] = batch_no
            row["pending"] = rec.get("status") == "排队中"
            row["abnormal"] = rec.get("status") == "已拒绝"
            if rec.get("status") in ("已拒绝", "已停止", "已完成"):
                row["结束时间"] = _today()

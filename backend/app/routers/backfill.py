"""数据回填接口：资源准入与排队结算。

结算批次、快照查询、导出等静态路由注册在 `/{entry_id}` 之前，避免被详情路由截获。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult, SettlePayload
from app.services.backfill import BackfillService

router = APIRouter(prefix="/api/backfill", tags=["数据回填"])

service = BackfillService()

LIST_FIELDS = ["回填编号", "作业名称", "目标数据表", "业务面", "分区积压量", "积压更新时间", "匹配资源池", "占用并发数", "结算批次号", "准入结果", "准入说明"]
STATUSES = ["待结算", "排队中", "已准入", "已拒绝", "已停止", "已完成"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按回填编号检索"),
    status: str | None = Query(default=None, description="待结算、排队中、已准入、已拒绝、已停止、已完成"),
    job: str | None = Query(default=None, description="按作业名称检索"),
    plane: str | None = Query(default=None, description="按业务面检索"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按回填编号、作业名称、业务面与状态过滤回填记录；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, job=job, plane=plane, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/settle", response_model=ActionResult)
def settle_batch(payload: SettlePayload) -> ActionResult:
    """多选回填记录一次提交形成结算批次：归并最新分区积压与资源池配额统一结算，
    生成资源准入数据快照并回写运行实例清单；超限记录拒绝准入，重复提交只生效一次。"""
    ids = [entry_id for entry_id in payload.ids if isinstance(entry_id, int)]
    if not ids:
        return ActionResult(ok=False, message="请先勾选要结算的回填记录，再提交结算批次")
    summary, message = service.settle_batch(ids)
    return ActionResult(ok=True, message=message, entry=summary)


@router.get("/snapshot")
def latest_snapshot() -> dict[str, Any]:
    """读取最新资源准入数据快照：各资源池槽位占用与最近批次的准入结果。"""
    snapshot = service.latest_snapshot()
    if snapshot is None:
        return {
            "快照编号": "",
            "结算批次号": "",
            "结算时间": "",
            "资源池占用": [],
            "准入结果": [],
            "提示": "尚未生成资源准入数据快照，请先勾选回填记录并提交结算批次",
        }
    return snapshot


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出数据回填清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "backfill", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条回填记录明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"回填记录 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条回填记录，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="回填记录已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条回填记录执行停止回填、标记完成；仅已准入记录可执行，槽位随动作释放。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/backlog", response_model=ActionResult)
def update_backlog(entry_id: int, payload: EntryPayload) -> ActionResult:
    """更新分区积压量：较晚到达的积压更新会改变优先级与并发占用，
    已准入记录立即按最新积压重新结算并刷新快照。"""
    entry, message = service.update_backlog(entry_id, payload.values.get("分区积压量"))
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)

<template>
  <section class="page" data-module="backfill">
    <header class="page-head">
      <div>
        <h2>数据回填管理</h2>
        <p class="page-desc">归并回填作业的分区积压与资源池配额，统一结算资源准入并生成快照，运行实例按结算结果启停回填。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" :disabled="!selectedIds.length" @click="settleBatch">
          提交结算批次{{ selectedIds.length ? `（已选 ${selectedIds.length} 条）` : '' }}
        </button>
        <button class="btn" type="button" @click="goRuntime">返回运行实例清单</button>
        <button class="btn" type="button" @click="exportRows">导出数据回填清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th>
            <input
              type="checkbox"
              :checked="allSettleableSelected"
              :disabled="!settleableRows.length"
              title="全选可结算记录"
              @change="toggleAll"
            />
          </th>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td>
            <input
              type="checkbox"
              :checked="selectedIds.includes(Number(row.id))"
              :disabled="!isSettleable(row)"
              :title="isSettleable(row) ? '选择加入结算批次' : '已准入或已完成，重复提交只生效一次'"
              @change="toggleRow(row)"
            />
          </td>
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="updateBacklog(row)">更新积压</button>
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 2" class="empty-state">暂无数据回填记录</td>
        </tr>
      </tbody>
    </table>

    <section v-if="snapshot && snapshot.快照编号" class="snapshot-panel">
      <header class="snapshot-head">
        <h3>资源准入数据快照 {{ snapshot.快照编号 }}</h3>
        <span>结算批次 {{ snapshot.结算批次号 }} · {{ snapshot.结算时间 }}</span>
      </header>
      <table class="data-table">
        <thead>
          <tr>
            <th>池编号</th>
            <th>池名称</th>
            <th>最大并发数</th>
            <th>已占用并发</th>
            <th>剩余槽位</th>
            <th>占用明细（回填编号 / 业务面 / 占用并发 / 分区积压）</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="pool in snapshot.资源池占用" :key="pool.池编号">
            <td>{{ pool.池编号 }}</td>
            <td>{{ pool.池名称 }}</td>
            <td>{{ pool.最大并发数 }}</td>
            <td>{{ pool.已占用并发 }}</td>
            <td>{{ pool.剩余槽位 }}</td>
            <td>
              <span v-if="!pool.占用明细.length">—</span>
              <span v-for="item in pool.占用明细" :key="item.回填编号" class="occupant-tag">
                {{ item.回填编号 }} · {{ item.业务面 }} · {{ item.占用并发数 }} 槽 · 积压 {{ item.分区积压量 }}
              </span>
            </td>
          </tr>
        </tbody>
      </table>
      <table v-if="snapshot.准入结果.length" class="data-table">
        <thead>
          <tr>
            <th>回填编号</th>
            <th>作业名称</th>
            <th>准入结果</th>
            <th>匹配资源池</th>
            <th>占用并发数</th>
            <th>分区积压量</th>
            <th>说明</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in snapshot.准入结果" :key="item.回填编号">
            <td>{{ item.回填编号 }}</td>
            <td>{{ item.作业名称 }}</td>
            <td>{{ item.准入结果 }}</td>
            <td>{{ item.匹配资源池 }}</td>
            <td>{{ item.占用并发数 }}</td>
            <td>{{ item.分区积压量 }}</td>
            <td>{{ item.说明 }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条数据回填记录</span>
      <span v-if="message" class="ok-text">{{ message }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type SnapshotOccupant = { 回填编号: string; 作业名称: string; 业务面: string; 占用并发数: number; 分区积压量: number }
type SnapshotPool = { 池编号: string; 池名称: string; 最大并发数: number; 已占用并发: number; 剩余槽位: number; 占用明细: SnapshotOccupant[] }
type AdmissionResult = { 回填编号: string; 作业名称: string; 准入结果: string; 匹配资源池: string; 占用并发数: number; 分区积压量: number; 说明: string }
type Snapshot = { 快照编号: string; 结算批次号: string; 结算时间: string; 资源池占用: SnapshotPool[]; 准入结果: AdmissionResult[] }

const ENDPOINT = '/api/backfill'
const columns = ["回填编号", "作业名称", "目标数据表", "业务面", "分区积压量", "积压更新时间", "匹配资源池", "占用并发数", "结算批次号", "准入结果", "准入说明"]
const actions = ["停止回填", "标记完成"]
const SETTLEABLE_STATUSES = ["待结算", "排队中", "已拒绝", "已停止"]

const router = useRouter()
const rows = ref<Row[]>([])
const total = ref(0)
const message = ref('')
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = ["回填编号", "作业名称", "业务面", "状态"]
const selectedIds = ref<number[]>([])
const snapshot = ref<Snapshot | null>(null)

const stats = computed(() => [
  { label: '回填记录总数', value: total.value },
  { label: '已准入', value: rows.value.filter((row) => row.status === '已准入').length },
  { label: '排队中', value: rows.value.filter((row) => row.status === '排队中').length },
  { label: '已拒绝', value: rows.value.filter((row) => row.status === '已拒绝').length },
])

const settleableRows = computed(() => rows.value.filter((row) => isSettleable(row)))
const allSettleableSelected = computed(
  () => settleableRows.value.length > 0 && settleableRows.value.every((row) => selectedIds.value.includes(Number(row.id))),
)

function isSettleable(row: Row) {
  return SETTLEABLE_STATUSES.includes(String(row.status))
}

function toggleRow(row: Row) {
  const id = Number(row.id)
  selectedIds.value = selectedIds.value.includes(id)
    ? selectedIds.value.filter((item) => item !== id)
    : [...selectedIds.value, id]
}

function toggleAll() {
  selectedIds.value = allSettleableSelected.value ? [] : settleableRows.value.map((row) => Number(row.id))
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function goRuntime() {
  void router.push('/runtime')
}

function buildQuery() {
  const params = new URLSearchParams()
  if (filters.value['回填编号']) params.set('keyword', filters.value['回填编号'])
  if (filters.value['作业名称']) params.set('job', filters.value['作业名称'])
  if (filters.value['业务面']) params.set('plane', filters.value['业务面'])
  if (filters.value['状态']) params.set('status', filters.value['状态'])
  params.set('size', '100')
  return params.toString()
}

async function settleBatch() {
  message.value = ''
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/settle`, {
      method: 'POST',
      body: JSON.stringify({ ids: selectedIds.value }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '结算批次提交失败')
    }
    message.value = payload.message
    selectedIds.value = []
    await Promise.all([reload(), loadSnapshot()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结算批次提交失败'
  }
}

async function updateBacklog(row: Row) {
  message.value = ''
  errorMessage.value = ''
  const input = window.prompt(`更新 ${row['回填编号']} 的最新分区积压量`, String(row['分区积压量'] ?? 0))
  if (input === null) {
    return
  }
  try {
    const response = await request(`${ENDPOINT}/${row.id}/backlog`, {
      method: 'POST',
      body: JSON.stringify({ values: { 分区积压量: input } }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '分区积压量更新失败')
    }
    message.value = payload.message
    await Promise.all([reload(), loadSnapshot()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区积压量更新失败'
  }
}

async function runAction(action: string, row: Row) {
  message.value = ''
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '数据回填动作未生效，请稍后重试')
    }
    message.value = payload.message
    await Promise.all([reload(), loadSnapshot()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据回填操作失败'
  }
}

async function loadSnapshot() {
  try {
    const response = await request(`${ENDPOINT}/snapshot`)
    if (response.ok) {
      snapshot.value = (await response.json()) as Snapshot
    }
  } catch {
    snapshot.value = null
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}?${buildQuery()}`)
    if (!response.ok) {
      throw new Error('数据回填列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据回填列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void loadSnapshot()
})
</script>

<style scoped>
.snapshot-panel {
  margin-top: 16px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px;
}
.snapshot-head {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 8px;
  color: var(--muted);
  font-size: 12px;
}
.snapshot-head h3 {
  margin: 0;
  font-size: 14px;
  color: #1f2937;
}
.snapshot-panel .data-table + .data-table {
  margin-top: 10px;
}
.occupant-tag {
  display: inline-block;
  margin: 2px 4px 2px 0;
  padding: 2px 6px;
  border: 1px solid var(--border);
  border-radius: 4px;
  font-size: 12px;
}
.ok-text {
  color: #067647;
}
</style>

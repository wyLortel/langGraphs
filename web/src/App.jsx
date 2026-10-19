import React, { useCallback, useEffect, useRef, useState } from 'react'

// 화면은 두 가지 데이터 소스를 쓴다 (시나리오 D의 핵심):
//  1) SSE /api/events  : Redis Pub/Sub 진행 이벤트. 끊긴 동안의 이벤트는 다시 오지 않는다 (타임라인)
//  2) GET /api/state   : PostgreSQL + Redis + MinIO의 '현재 상태' 스냅샷. 언제 다시 읽어도 복구된다 (표들)

const pad = (n, w = 2) => String(n).padStart(w, '0')
const fmtTime = (ts) => {
  const d = new Date(ts * 1000)
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`
}
const short = (v) => (v == null ? '' : String(v).length > 10 ? String(v).slice(0, 8) + '…' : String(v))

async function api(path, opts = {}) {
  const res = await fetch(path, { headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) }, ...opts })
  const text = await res.text()
  let body
  try { body = JSON.parse(text) } catch { body = text }
  return { status: res.status, body }
}

function Json({ v }) {
  return <pre className="json">{JSON.stringify(v, null, 2)}</pre>
}

function Table({ rows, cols, empty = '(없음)', rowClass }) {
  if (!rows || rows.length === 0) return <div className="muted">{empty}</div>
  return (
    <div className="tablewrap">
      <table>
        <thead><tr>{cols.map((c) => <th key={c[0]}>{c[0]}</th>)}</tr></thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className={rowClass ? rowClass(r) : ''}>
              {cols.map((c) => <td key={c[0]}>{c[1](r)}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function Panel({ title, sub, children, className = '' }) {
  return (
    <section className={'panel ' + className}>
      <h2>{title} {sub && <small>{sub}</small>}</h2>
      {children}
    </section>
  )
}

// ------------------------------------------------------------------ 타임라인 (SSE)
function useEvents(paused) {
  const [events, setEvents] = useState([])
  const [connected, setConnected] = useState(false)
  const esRef = useRef(null)
  const add = (e) => setEvents((xs) => [...xs.slice(-600), e])

  useEffect(() => {
    if (paused) {
      if (esRef.current) {
        esRef.current.close()
        esRef.current = null
        setConnected(false)
        add({ marker: true, ts: Date.now() / 1000, text: '──── SSE 연결 끊음: 지금부터 발행되는 Pub/Sub 이벤트는 이 화면에 영원히 도착하지 않는다 ────' })
      }
      return
    }
    const es = new EventSource('/api/events')
    esRef.current = es
    es.onopen = () => {
      setConnected(true)
      add({ marker: true, ts: Date.now() / 1000, text: '──── SSE 연결됨 (Redis SUBSCRIBE events) ────' })
    }
    es.onmessage = (m) => { try { add(JSON.parse(m.data)) } catch { /* ignore */ } }
    es.onerror = () => setConnected(false)
    return () => es.close()
  }, [paused])
  return { events, connected, clear: () => setEvents([]) }
}

function Timeline({ events, filter }) {
  const [open, setOpen] = useState(null)
  const endRef = useRef(null)
  useEffect(() => { endRef.current?.scrollIntoView({ block: 'nearest' }) }, [events.length])
  const f = filter.trim().toLowerCase()
  const shown = events.filter((e) => !f || JSON.stringify(e).toLowerCase().includes(f))
  return (
    <div className="timeline">
      {shown.map((e, i) => e.marker ? (
        <div key={i} className="marker">[{fmtTime(e.ts)}] {e.text}</div>
      ) : (
        <div key={i} className={'ev ev-' + cls(e.event)} onClick={() => setOpen(open === i ? null : i)}>
          <span className="t">[{fmtTime(e.ts)}]</span>{' '}
          <span className="src">{e.source}</span>{' '}
          {e.route && <span className="route">{e.route}</span>}{' | '}
          <b>{e.event}</b>{' '}
          <span className="kv">{summary(e.data)}</span>
          {open === i && <Json v={e} />}
        </div>
      ))}
      <div ref={endRef} />
    </div>
  )
}

function cls(ev) {
  if (/CRASH|FAILED|DEAD|STALE|LOST|REJECT|ERROR/.test(ev)) return 'bad'
  if (/RECLAIM|TAKEOVER|DUPLICATE|SKIP|WARN/.test(ev)) return 'warn'
  if (/INTERRUPT|WAITING|RESUMED|DECISION/.test(ev)) return 'graph'
  if (/CHECKPOINT/.test(ev)) return 'cp'
  if (/XACK|COMMIT/.test(ev)) return 'ok'
  return ''
}

function summary(d) {
  if (!d) return ''
  const keys = ['thread_id', 'node', 'mode', 'job_id', 'msg_id', 'delivery', 'from_owner', 'checkpoint_id', 'step',
    'lease_token', 'key', 'tool', 'stream', 'status', 'inserted', 'newly_inserted', 'provider_accepted', 'exec_count', 'reason']
  return keys.filter((k) => d[k] !== undefined && d[k] !== null)
    .map((k) => `${k}=${typeof d[k] === 'object' ? JSON.stringify(d[k]) : (['job_id', 'checkpoint_id'].includes(k) ? short(d[k]) : d[k])}`)
    .join(' ')
}

// ------------------------------------------------------------------ 액션 (요청 보내기)
const CHAOS_PING = ['', 'crash_after_read', 'crash_before_ack', 'always_fail', 'fail_xadd', 'crash_after_xadd']
const CHAOS_CRAWL = ['', 'crash_after_db_write', 'crash_after_notify']
const CHAOS_APP = ['', 'crash_in_node', 'crash_after_upload', 'crash_after_read', 'crash_before_ack']

function Actions({ state, onResult }) {
  const [ping, setPing] = useState({ message: 'hello', key: '', chaos: '', sleep_sec: 0, lease: true, enqueue: 'outbox' })
  const [crawl, setCrawl] = useState({ chaos: '', notify_idempotency: true })
  const [app, setApp] = useState({ student_id: 1, scholarship_id: '', chaos: '', draft_key_mode: 'deterministic' })
  const approved = (state?.scholarships || []).filter((s) => s.status === 'approved')

  const send = async (label, path, body, headers) => {
    const r = await api(path, { method: 'POST', body: JSON.stringify(body), headers })
    onResult({ label, path, request: body, headers, ...r })
  }

  return (
    <div className="actions">
      <fieldset>
        <legend>① 가장 단순한 흐름: POST /api/ping</legend>
        <label>message <input value={ping.message} onChange={(e) => setPing({ ...ping, message: e.target.value })} /></label>
        <label title="같은 값을 두 번 보내면 중복 요청">Idempotency-Key <input placeholder="(비우면 매번 새 키)" value={ping.key} onChange={(e) => setPing({ ...ping, key: e.target.value })} /></label>
        <label>chaos <select value={ping.chaos} onChange={(e) => setPing({ ...ping, chaos: e.target.value })}>{CHAOS_PING.map((c) => <option key={c} value={c}>{c || '(없음)'}</option>)}</select></label>
        <label>sleep_sec <input type="number" style={{ width: 50 }} value={ping.sleep_sec} onChange={(e) => setPing({ ...ping, sleep_sec: +e.target.value })} /></label>
        <label><input type="checkbox" checked={ping.lease} onChange={(e) => setPing({ ...ping, lease: e.target.checked })} /> lease 갱신</label>
        <label>enqueue <select value={ping.enqueue} onChange={(e) => setPing({ ...ping, enqueue: e.target.value })}><option>outbox</option><option>direct</option></select></label>
        <button onClick={() => send('ping', `/api/ping?enqueue=${ping.enqueue}`,
          { message: ping.message, chaos: ping.chaos || null, sleep_sec: ping.sleep_sec, lease: ping.lease },
          ping.key ? { 'Idempotency-Key': ping.key } : {})}>보내기</button>
      </fieldset>

      <fieldset>
        <legend>② 공고 수집 → 추천 (시나리오 A): POST /api/crawl</legend>
        <label>chaos <select value={crawl.chaos} onChange={(e) => setCrawl({ ...crawl, chaos: e.target.value })}>{CHAOS_CRAWL.map((c) => <option key={c} value={c}>{c || '(없음)'}</option>)}</select></label>
        <label><input type="checkbox" checked={crawl.notify_idempotency} onChange={(e) => setCrawl({ ...crawl, notify_idempotency: e.target.checked })} /> 알림 멱등 키 사용</label>
        <button onClick={() => send('crawl', '/api/crawl', { chaos: crawl.chaos || null, notify_idempotency: crawl.notify_idempotency })}>수집 실행</button>
      </fieldset>

      <fieldset>
        <legend>③ 신청 준비 (시나리오 B): POST /api/applications</legend>
        <label>학생 <select value={app.student_id} onChange={(e) => setApp({ ...app, student_id: +e.target.value })}>
          {(state?.students || []).map((s) => <option key={s.id} value={s.id}>{s.name}</option>)}</select></label>
        <label>장학금 <select value={app.scholarship_id} onChange={(e) => setApp({ ...app, scholarship_id: e.target.value })}>
          <option value="">(선택)</option>
          {approved.map((s) => <option key={s.id} value={s.id}>#{s.id} {s.title}</option>)}</select></label>
        <label>chaos <select value={app.chaos} onChange={(e) => setApp({ ...app, chaos: e.target.value })}>{CHAOS_APP.map((c) => <option key={c} value={c}>{c || '(없음)'}</option>)}</select></label>
        <label>초안 키 <select value={app.draft_key_mode} onChange={(e) => setApp({ ...app, draft_key_mode: e.target.value })}><option>deterministic</option><option>random</option></select></label>
        <button disabled={!app.scholarship_id} onClick={() => send('apply', '/api/applications',
          { student_id: app.student_id, scholarship_id: +app.scholarship_id, chaos: app.chaos || null, draft_key_mode: app.draft_key_mode })}>신청 준비</button>
        {approved.length === 0 && <div className="muted">먼저 ②수집을 실행해 승인된 공고를 만드세요.</div>}
      </fieldset>
    </div>
  )
}

function AnswerForm({ app, onResult }) {
  const fields = app.pending_question?.fields || []
  const [vals, setVals] = useState({})
  const submit = async () => {
    const answers = {}
    fields.forEach((f) => { answers[f] = f === 'volunteer_hours' ? Number(vals[f] || 0) : (vals[f] || '') })
    const r = await api(`/api/applications/${app.id}/answer`, { method: 'POST', body: JSON.stringify({ answers }) })
    onResult({ label: 'answer', path: `/api/applications/${app.id}/answer`, request: { answers }, ...r })
  }
  return (
    <div className="answer">
      <div>❓ {app.pending_question?.question}</div>
      {fields.map((f) => (
        <label key={f}>{f} <input value={vals[f] || ''} onChange={(e) => setVals({ ...vals, [f]: e.target.value })} /></label>
      ))}
      <button onClick={submit}>답변 → resume</button>
    </div>
  )
}

// ------------------------------------------------------------------ 메인
export default function App() {
  const [paused, setPaused] = useState(false)
  const { events, connected, clear } = useEvents(paused)
  const [state, setState] = useState(null)
  const [lastResult, setLastResult] = useState(null)
  const [thread, setThread] = useState(null)
  const [filter, setFilter] = useState('')

  const load = useCallback(async () => {
    const r = await api('/api/state')
    if (r.status === 200) setState(r.body)
  }, [])

  // '브라우저를 닫은 상태' 흉내: SSE도 끊고 상태 조회도 멈춘다. 재연결하면 /api/state로 한 번에 복구.
  useEffect(() => {
    if (paused) return
    load()
    const t = setInterval(load, 2000)
    return () => clearInterval(t)
  }, [paused, load])

  const [threadData, setThreadData] = useState(null)
  useEffect(() => {
    if (!thread) return
    const f = async () => { const r = await api(`/api/threads/${thread}`); if (r.status === 200) setThreadData(r.body) }
    f()
    const t = setInterval(f, 2000)
    return () => clearInterval(t)
  }, [thread])

  const disconnect15 = () => {
    setPaused(true)
    setTimeout(() => setPaused(false), 15000)
  }

  const onResult = (r) => { setLastResult(r); load() }
  const redis = state?.redis || {}

  return (
    <div className="app">
      <header>
        <h1>Scholarship Lab <small>아키텍처 관찰 화면</small></h1>
        <span className={'pill ' + (connected ? 'on' : 'off')}>SSE {connected ? '연결됨' : paused ? '끊김(실험 중)' : '연결 안 됨'}</span>
        <button onClick={disconnect15} disabled={paused}>SSE + 화면 갱신 15초 끊기 (시나리오 D)</button>
        <button onClick={clear}>타임라인 지우기</button>
        <button className="danger" onClick={async () => { await api('/api/admin/reset', { method: 'POST' }); clear(); load() }}>전체 초기화</button>
        <span className="muted">경로: Browser → nginx(web:80) → api:8000 → PostgreSQL / Redis → Worker</span>
      </header>

      <div className="grid">
        <Panel title="살아 있는 Worker" sub="Redis heartbeat:* 키 (TTL 10초) — 끊기면 10초 뒤 사라짐" className="span2">
          <Table rows={state?.heartbeats} cols={[
            ['worker_id', (h) => h.worker_id], ['container', (h) => h.container], ['pid', (h) => h.pid],
            ['started', (h) => fmtTime(h.started_at)], ['stream', (h) => h.stream],
            ['in_flight', (h) => h.in_flight], ['current_job', (h) => short(h.current_job)], ['current_thread', (h) => h.current_thread],
            ['current_node', (h) => h.current_node],
            ['graph_obj_ids', (h) => h.graph_obj_ids ? Object.entries(h.graph_obj_ids).map(([k, v]) => `${k}:${v}`).join(' ') : ''],
          ]} />
        </Panel>

        <Panel title="요청 보내기" className="span2">
          <Actions state={state} onResult={onResult} />
          {lastResult && <details open><summary>마지막 응답 HTTP {lastResult.status} — {lastResult.path}</summary><Json v={lastResult} /></details>}
        </Panel>

        <Panel title="타임라인" sub="Redis Pub/Sub → SSE (저장되지 않는 신호, 클릭하면 JSON)" className="span2 tall">
          <input className="filter" placeholder="필터 (예: app:1, ai-worker-2, CHECKPOINT)" value={filter} onChange={(e) => setFilter(e.target.value)} />
          <Timeline events={events} filter={filter} />
        </Panel>

        {['ai_jobs', 'crawler_jobs'].map((s) => {
          const x = redis[s] || {}
          const g = (x.groups || [])[0] || {}
          return (
            <Panel key={s} title={`Redis Stream: ${s}`} sub={`XLEN=${x.length ?? '-'} (ACK된 기록 포함) · group=${g.name || '-'} pending=${g.pending ?? '-'} lag=${g.lag ?? '-'}`}>
              <h3>Consumers (XINFO CONSUMERS)</h3>
              <Table rows={x.consumers} cols={[['name', (c) => c.name], ['pending', (c) => c.pending], ['idle(ms)', (c) => c.idle]]} />
              <h3>Pending — 받았지만 아직 ACK 안 된 메시지 (XPENDING)</h3>
              <Table rows={x.pending} empty="(없음: 모두 ACK됨)" cols={[
                ['msg_id', (p) => p.message_id], ['owner', (p) => p.consumer], ['idle(ms)', (p) => p.time_since_delivered],
                ['전달 횟수', (p) => p.times_delivered], ['type', (p) => p.type], ['job', (p) => short(p.job_id)]]} />
              <h3>DLQ ({s}:dlq)</h3>
              <Table rows={x.dlq} cols={[['msg_id', (d) => d.msg_id], ['orig', (d) => d.orig_msg_id], ['deliveries', (d) => d.deliveries], ['reason', (d) => d.reason]]} />
            </Panel>
          )
        })}

        <Panel title="PostgreSQL: applications" sub="신청 상태 + 대기 중인 질문 (재접속 시 이 표로 질문을 복구)" className="span2">
          <Table rows={state?.applications} cols={[
            ['id', (a) => a.id], ['student', (a) => a.student_id], ['scholarship', (a) => a.scholarship_id],
            ['thread_id', (a) => <a href="#" onClick={(e) => { e.preventDefault(); setThread(a.thread_id); setThreadData(null) }}>{a.thread_id}</a>],
            ['status', (a) => <b className={'st-' + a.status}>{a.status}</b>],
            ['options', (a) => JSON.stringify(a.options)],
            ['질문 / 결과', (a) => a.status === 'waiting_input' ? <AnswerForm app={a} onResult={onResult} /> :
              a.draft_s3_key ? <a href="#" onClick={async (e) => {
                e.preventDefault()
                const r = await api(`/api/applications/${a.id}/draft`)
                onResult({ label: 'draft', path: `/api/applications/${a.id}/draft`, ...r })
                if (r.body?.url) window.open(r.body.url, '_blank')
              }}>초안 다운로드 (presigned URL)</a> : ''],
          ]} />
        </Panel>

        {thread && (
          <Panel title={`LangGraph thread: ${thread}`} sub="checkpoints 테이블을 SQL로 직접 조회 · 노드 실행 횟수(Redis)" className="span2">
            <button onClick={() => setThread(null)}>닫기</button>
            <div>노드 실행 횟수: {threadData && Object.entries(threadData.node_exec_counts).map(([k, v]) =>
              <span key={k} className={'chip ' + (+v > 1 ? 'warn' : '')}>{k} ×{v}</span>)}</div>
            <Table rows={threadData?.checkpoints} cols={[
              ['step', (c) => c.step], ['source', (c) => c.source], ['checkpoint_id', (c) => c.checkpoint_id],
              ['parent', (c) => short(c.parent_checkpoint_id)], ['다음에 실행될 노드(task)', (c) => (c.write_tasks || []).join(', ')],
              ['쓰여진 채널', (c) => (c.write_channels || []).join(', ')]]} />
          </Panel>
        )}

        <Panel title="PostgreSQL: jobs" sub="작업의 최종 운명 (Redis 메시지는 ACK되면 사라지지만 이 행은 남는다)" className="span2">
          <Table rows={state?.jobs} rowClass={(j) => j.orphaned ? 'orphan' : ''} cols={[
            ['job', (j) => short(j.job_id)], ['type', (j) => j.type], ['status', (j) => <b className={'st-' + j.status}>{j.status}{j.orphaned ? ' (고아!)' : ''}</b>],
            ['attempts', (j) => j.attempts], ['실행 횟수', (j) => j.runs], ['worker', (j) => j.worker_id], ['lease_token', (j) => j.lease_token],
            ['idempotency_key', (j) => j.idempotency_key], ['error', (j) => j.error]]} />
        </Panel>

        <Panel title="PostgreSQL: job_runs" sub="실행 시도 1번 = 1행 (중복 실행이 데이터로 보임)">
          <Table rows={state?.job_runs} cols={[
            ['run', (r) => r.run_id], ['job', (r) => short(r.job_id)], ['worker', (r) => r.worker_id], ['pid', (r) => r.pid],
            ['token', (r) => r.lease_token], ['delivery', (r) => r.delivery], ['outcome', (r) => <b className={'st-' + r.outcome}>{r.outcome}</b>]]} />
        </Panel>

        <Panel title="PostgreSQL: outbox" sub="published_at이 비어 있으면 아직 Redis에 안 들어감">
          <Table rows={state?.outbox} cols={[
            ['id', (o) => o.id], ['stream', (o) => o.stream], ['job', (o) => short(o.job_id)],
            ['published', (o) => o.published_at ? '✔' : '⏳ 대기'], ['msg_id', (o) => o.stream_msg_id]]} />
        </Panel>

        <Panel title="PostgreSQL: scholarships + pgvector" sub={`embedding 행 수: ${state?.embeddings ?? '-'}`}>
          <Table rows={state?.scholarships} cols={[
            ['id', (s) => s.id], ['notice', (s) => s.notice_id], ['title', (s) => s.title], ['status', (s) => <b className={'st-' + s.status}>{s.status}</b>],
            ['hash', (s) => s.content_hash?.slice(0, 8)], ['원문 위치(MinIO)', (s) => s.s3_key],
            ['문제', (s) => (s.problems || []).map((p) => p.problem).join(', ')]]} />
        </Panel>

        <Panel title="PostgreSQL: recommendations / 알림">
          <Table rows={state?.recommendations} cols={[['student', (r) => r.student_id], ['scholarship', (r) => r.scholarship_id], ['verdict', (r) => r.verdict], ['reasons', (r) => (r.reasons || []).join(' · ')]]} />
          <h3>notifications (우리 쪽 멱등 레코드)</h3>
          <Table rows={state?.notifications} cols={[['student', (n) => n.student_id], ['scholarship', (n) => n.scholarship_id], ['status', (n) => n.status], ['attempts', (n) => n.attempts]]} />
          <h3>mock_provider_inbox (MOCK 외부 제공자가 실제로 받은 것 = 학생 휴대폰)</h3>
          <Table rows={state?.provider_inbox} cols={[['id', (n) => n.id], ['idempotency_key', (n) => n.idempotency_key || '(없음)'], ['recipient', (n) => n.recipient]]} />
        </Panel>

        <Panel title="MinIO (S3 대체): 버킷 객체" sub="원문 raw/ · 초안 drafts/ · .queued = Crawler가 큐에 넣었다는 표시">
          <Table rows={state?.minio} cols={[['key', (o) => o.key], ['bytes', (o) => o.size]]} />
        </Panel>

        <Panel title="PostgreSQL: ping_results">
          <Table rows={state?.ping_results} cols={[['id', (p) => p.id], ['job', (p) => short(p.job_id)], ['message', (p) => p.message], ['worker', (p) => p.worker_id]]} />
        </Panel>
      </div>
    </div>
  )
}

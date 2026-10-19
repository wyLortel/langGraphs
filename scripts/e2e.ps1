# 전체 시나리오 통합 검증 (Windows PowerShell 5.1 호환)
# 사용법:  docker compose up -d --build   후   powershell -ExecutionPolicy Bypass -File scripts\e2e.ps1
# 소요 시간: 약 5분 (XAUTOCLAIM min-idle 15초 대기가 여러 번 있음)
# 주의: 시작할 때 /api/admin/reset 으로 모든 데이터를 지운다.

$ErrorActionPreference = "Continue"   # docker CLI가 stderr에 진행 상황을 써도 멈추지 않게
$Api = "http://localhost:8000"
$script:Fail = 0
Set-Location (Split-Path $PSScriptRoot -Parent)

function Post($path, $body, $headers = @{}) {
    # WHY 바이트로 보냄: PowerShell 5.1은 문자열 본문을 UTF-8로 보내지 않아 한글이 깨진다
    $bytes = [System.Text.Encoding]::UTF8.GetBytes(($body | ConvertTo-Json -Compress -Depth 5))
    try {
        return Invoke-RestMethod -Method Post -Uri "$Api$path" -Body $bytes -ContentType "application/json; charset=utf-8" -Headers $headers
    } catch {
        return @{ error = $_.Exception.Message }
    }
}
function Sql($q) { (docker compose exec -T postgres psql -U lab -d scholarship -Atc $q) -join "`n" }
function Check($name, $actual, $expected) {
    if ("$actual" -eq "$expected") { Write-Host "  [PASS] $name = $actual" -ForegroundColor Green }
    else { Write-Host "  [FAIL] $name = '$actual' (expected '$expected')" -ForegroundColor Red; $script:Fail++ }
}
function Step($t) { Write-Host "`n=== $t ===" -ForegroundColor Cyan }

Step "0. 초기화"
Post "/api/admin/reset" @{} | Out-Null
Start-Sleep 3

Step "1. API -> outbox -> Redis -> Worker -> PostgreSQL (ping)"
$r1 = Post "/api/ping" @{ message = "hello" } @{ "Idempotency-Key" = "e2e-ping-1" }
$r2 = Post "/api/ping" @{ message = "hello" } @{ "Idempotency-Key" = "e2e-ping-1" }
Start-Sleep 4
Check "두 번째 요청 duplicate" $r2.duplicate "True"
Check "jobs 행 수(같은 키)" (Sql "select count(*) from jobs where idempotency_key='e2e-ping-1'") 1
Check "job 상태" (Sql "select status from jobs where idempotency_key='e2e-ping-1'") "done"
Check "ping_results 행 수" (Sql "select count(*) from ping_results p join jobs j using(job_id) where j.idempotency_key='e2e-ping-1'") 1

Step "C. Redis 장애: ACK 전 크래시 / DB 저장 후 ACK 전 크래시 / DLQ"
Post "/api/ping" @{ message = "c1"; chaos = "crash_after_read" } @{ "Idempotency-Key" = "e2e-c-read" } | Out-Null
Post "/api/ping" @{ message = "c2"; chaos = "crash_before_ack" } @{ "Idempotency-Key" = "e2e-c-ack" } | Out-Null
Post "/api/ping" @{ message = "c3"; chaos = "always_fail" } @{ "Idempotency-Key" = "e2e-c-dlq" } | Out-Null
Write-Host "  ... XAUTOCLAIM 재시도 대기 (75초)"
Start-Sleep 75
Check "crash_after_read -> 회수 후 done" (Sql "select status from jobs where idempotency_key='e2e-c-read'") "done"
Check "crash_before_ack -> 결과 1행" (Sql "select count(*) from ping_results p join jobs j using(job_id) where j.idempotency_key='e2e-c-ack'") 1
Check "crash_before_ack -> 재전달은 already_done" (Sql "select string_agg(outcome, ',' order by run_id) from job_runs r join jobs j using(job_id) where j.idempotency_key='e2e-c-ack'") "committed,already_done"
Check "always_fail -> dead" (Sql "select status from jobs where idempotency_key='e2e-c-dlq'") "dead"
Check "DLQ 길이" (docker compose exec -T redis redis-cli -a labredis --no-auth-warning xlen ai_jobs:dlq) 1

Step "G. DB commit과 XADD 사이 장애"
Post "/api/ping?enqueue=direct" @{ message = "orphan"; chaos = "fail_xadd" } @{ "Idempotency-Key" = "e2e-g-direct" } | Out-Null
docker compose stop outbox-relay 2>$null | Out-Null
Post "/api/ping" @{ message = "relay down" } @{ "Idempotency-Key" = "e2e-g-down" } | Out-Null
Start-Sleep 3
Check "relay 정지 중 outbox 미발행" (Sql "select count(*) from outbox o join jobs j using(job_id) where j.idempotency_key='e2e-g-down' and o.published_at is null") 1
docker compose start outbox-relay 2>$null | Out-Null
Post "/api/ping" @{ message = "dup xadd"; chaos = "crash_after_xadd" } @{ "Idempotency-Key" = "e2e-g-dup" } | Out-Null
Start-Sleep 12
Check "direct+fail_xadd -> 고아(created)" (Sql "select status from jobs where idempotency_key='e2e-g-direct'") "created"
Check "relay 재시작 후 자동 처리" (Sql "select status from jobs where idempotency_key='e2e-g-down'") "done"
Check "중복 XADD 흡수 (실행 기록)" (Sql "select string_agg(outcome, ',' order by run_id) from job_runs r join jobs j using(job_id) where j.idempotency_key='e2e-g-dup'") "committed,already_done"

Step "A+E. 수집 -> MinIO -> LangGraph(MCP, Mock LLM) -> 추천"
Post "/api/crawl" @{} | Out-Null
Start-Sleep 10
Check "scholarships 상태" (Sql "select string_agg(notice_id||':'||status, ',' order by notice_id) from scholarships") "busan-2026:approved,hope-2026:approved,science-2026:needs_review"
Check "학생B-부산 추천" (Sql "select verdict from recommendations r join scholarships s on s.id=r.scholarship_id where r.student_id=2 and s.notice_id='busan-2026'") "eligible"
Check "MCP tools/call 로그 존재" ([bool](docker compose logs mcp 2>$null | Select-String "tools/call")) "True"
Post "/api/crawl" @{} | Out-Null
Start-Sleep 5
Check "재수집해도 공고 수 그대로(변경 감지)" (Sql "select count(*) from scholarships") 3

Step "B. interrupt -> Worker 재시작 -> resume"
$hope = Sql "select id from scholarships where notice_id='hope-2026'"
$busan = Sql "select id from scholarships where notice_id='busan-2026'"
$a = Post "/api/applications" @{ student_id = 1; scholarship_id = [int]$hope }
$b = Post "/api/applications" @{ student_id = 2; scholarship_id = [int]$busan }
Start-Sleep 5
Check "두 신청 모두 질문 대기" (Sql "select string_agg(status, ',' order by id) from applications") "waiting_input,waiting_input"
docker compose restart ai-worker-1 ai-worker-2 2>$null | Out-Null
Start-Sleep 5
Post "/api/applications/$($a.application_id)/answer" @{ answers = @{ volunteer_hours = 25; motivation = "교육 격차를 줄이고 싶습니다" } } | Out-Null
$dbl = Post "/api/applications/$($a.application_id)/answer" @{ answers = @{ motivation = "double" } }
Post "/api/applications/$($b.application_id)/answer" @{ answers = @{ motivation = "부산 창업" } } | Out-Null
Start-Sleep 8
Check "Resume Gate 이중 제출 거부" ([bool]($dbl.error -match "409")) "True"
Check "두 신청 완료" (Sql "select string_agg(status, ',' order by id) from applications") "done,done"
Check "ask_student 실행 횟수(재개 시 재실행)" (docker compose exec -T redis redis-cli -a labredis --no-auth-warning hget "node_exec:app:$($a.application_id)" ask_student) 2
$d = Invoke-RestMethod "$Api/api/applications/$($a.application_id)/draft"
Check "presigned URL로 초안 다운로드" ([bool]((Invoke-WebRequest -UseBasicParsing $d.url).StatusCode -eq 200)) "True"

Step "H. 노드 실행 중 크래시와 부작용"
$c = Post "/api/applications" @{ student_id = 1; scholarship_id = [int]$busan; chaos = "crash_after_upload"; draft_key_mode = "random" }
Start-Sleep 5
Post "/api/applications/$($c.application_id)/answer" @{ answers = @{ motivation = "random key" } } | Out-Null
Write-Host "  ... 크래시 후 회수 대기 (25초)"
Start-Sleep 25
Check "write_draft 재실행 횟수" (docker compose exec -T redis redis-cli -a labredis --no-auth-warning hget "node_exec:app:$($c.application_id)" write_draft) 2
$objs = (Invoke-RestMethod "$Api/api/state").minio | Where-Object { $_.key -like "drafts/app-$($c.application_id)/*" }
Check "random 키 -> MinIO 고아 파일 포함 2개" (@($objs).Count) 2

Step "F. 느린 Worker vs 죽은 Worker (docker pause)"
Post "/api/ping" @{ message = "pause me"; sleep_sec = 20; lease = $true } @{ "Idempotency-Key" = "e2e-f-pause" } | Out-Null
Start-Sleep 3
$busy = ((Invoke-RestMethod "$Api/api/state").heartbeats | Where-Object { $_.current_job }).worker_id | Select-Object -First 1
Write-Host "  작업 중인 Worker: $busy -> 30초 동안 pause"
docker compose pause $busy 2>$null | Out-Null
Start-Sleep 30
docker compose unpause $busy 2>$null | Out-Null
Start-Sleep 25
Check "실행 기록 (얼린 Worker는 거부됨)" (Sql "select string_agg(outcome, ',' order by run_id) from job_runs r join jobs j using(job_id) where j.idempotency_key='e2e-f-pause'") "stale_rejected,committed"
Check "결과 1행" (Sql "select count(*) from ping_results p join jobs j using(job_id) where j.idempotency_key='e2e-f-pause'") 1

Write-Host ""
if ($script:Fail -eq 0) { Write-Host "ALL PASSED" -ForegroundColor Green } else { Write-Host "$($script:Fail) FAILED" -ForegroundColor Red; exit 1 }

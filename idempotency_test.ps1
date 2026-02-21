# idempotency_test.ps1
# Sends the SAME transaction twice (same idempotency_key). Second call must NOT change balances.

$baseUrl = "http://localhost:8080"   # <-- change if your ledger API port differs
$endpoint = "$baseUrl/transactions"  # <-- change if your route differs

$body = @{
  idempotency_key = "idem-test-001"
  entries = @(
    @{ account_id = "A"; direction = "debit";  amount = 7 }
    @{ account_id = "B"; direction = "credit"; amount = 7 }
  )
} | ConvertTo-Json -Depth 5

Write-Host "POST #1"
Invoke-RestMethod -Method Post -Uri $endpoint -ContentType "application/json" -Body $body

Write-Host "POST #2 (duplicate)"
Invoke-RestMethod -Method Post -Uri $endpoint -ContentType "application/json" -Body $body

Write-Host "Done. Now run DB checks:"
Write-Host "  docker exec -it ledgerflow-postgres psql -U postgres -d ledgerflow"
Write-Host "  SELECT COUNT(*) FROM read_model.processed_events;"
Write-Host "  SELECT * FROM read_model.account_balances ORDER BY account_id;"
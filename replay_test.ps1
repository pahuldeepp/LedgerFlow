# replay_test.ps1
docker compose stop read-model-builder | Out-Host

$env:REBUILD="true"
docker compose up -d --force-recreate read-model-builder | Out-Host
Remove-Item Env:REBUILD

Start-Sleep -Seconds 3

$queries = @"
\pset pager off
SELECT now() AS ts;
SELECT SUM(balance) AS total_system_balance FROM read_model.account_balances;
SELECT
  SUM(CASE WHEN balance > 0 THEN balance ELSE 0 END) AS total_credits,
  SUM(CASE WHEN balance < 0 THEN -balance ELSE 0 END) AS total_debits
FROM read_model.account_balances;
SELECT COUNT(*) AS processed_events FROM read_model.processed_events;
SELECT COUNT(*) AS tx_feed FROM read_model.tx_feed;
"@

$queries | docker exec -i ledgerflow-postgres psql -U postgres -d ledgerflow -v ON_ERROR_STOP=1
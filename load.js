import http from 'k6/http';
import { sleep } from 'k6';

export const options = {
  vus: 20,
  duration: '30s',
};

export default function () {
  const payload = JSON.stringify({
    idempotency_key: crypto.randomUUID(),
    entries: [
      { account_id: "A1", direction: "debit", amount: 100 },
      { account_id: "A2", direction: "credit", amount: 100 }
    ]
  });

  http.post('http://localhost:8080/transactions', payload, {
    headers: { 'Content-Type': 'application/json' }
  });

  sleep(0.1);
}

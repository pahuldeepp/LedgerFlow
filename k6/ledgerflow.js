import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  scenarios: {
    warmup: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "20s", target: 5 },
        { duration: "20s", target: 5 },
      ],
      gracefulRampDown: "5s",
    },
    steady: {
      executor: "constant-vus",
      vus: 20,
      duration: "60s",
      startTime: "45s",
    },
    spike: {
      executor: "ramping-vus",
      startVUs: 20,
      stages: [
        { duration: "10s", target: 60 },
        { duration: "20s", target: 60 },
        { duration: "10s", target: 20 },
      ],
      startTime: "110s",
      gracefulRampDown: "5s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],          // < 1% failures
    http_req_duration: ["p(95)<500"],        // p95 under 500ms (tune later)
  },
};

const BASE_URL = __ENV.BASE_URL || "http://host.docker.internal:8080";

function randId() {
  // k6-safe random string
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

export default function () {
  const key = `k6-${randId()}`;

  const payload = JSON.stringify({
    idempotency_key: key,
    entries: [
      { account_id: "acc_1", direction: "debit", amount: 10 },
      { account_id: "acc_2", direction: "credit", amount: 10 },
    ],
  });

  const res = http.post(`${BASE_URL}/transactions`, payload, {
    headers: { "Content-Type": "application/json" },
    timeout: "10s",
  });

  check(res, {
    "status is 200/201": (r) => r.status === 200 || r.status === 201,
  });

  sleep(0.1);
}
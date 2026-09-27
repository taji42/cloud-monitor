# Cloud Uptime & Performance Monitor

A lightweight, cloud-native uptime monitoring system built with Python, PostgreSQL, and GitHub Actions. It concurrently checks target endpoints, persists latency metrics, and dispatches real-time alerts to Discord on downtime.

![Pipeline Status](https://github.com/taji42/cloud-monitor/actions/workflows/monitor.yml/badge.svg)

---

## Key Features

* **Async Concurrent Polling:** Utilizes Python's `httpx` and `asyncio` to execute non-blocking HTTP health checks across multiple targets simultaneously.
* **Metric Persistence:** Connects to Supabase PostgreSQL via connection pooling (`psycopg2`) to record status codes, response latencies (ms), and failure reasons.
* **Automated Webhook Alerts:** Formats rich Discord webhook payloads to notify administrators immediately when an endpoint fails.
* **Zero-Server Cloud Execution:** Runs headlessly every 10 minutes using GitHub Actions CRON workflows, requiring zero infrastructure overhead.

---

## Tech Stack

* **Language:** Python 3.11+
* **HTTP Client:** `httpx` (Asynchronous)
* **Database:** Supabase PostgreSQL (`psycopg2-binary`)
* **Alerting:** Discord Webhooks API
* **Automation:** GitHub Actions CI/CD

---

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS monitors (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL,
    timeout_seconds INT DEFAULT 10,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ping_logs (
    id BIGSERIAL PRIMARY KEY,
    monitor_id INT REFERENCES monitors(id) ON DELETE CASCADE,
    status_code INT,
    response_time_ms INT,
    is_up BOOLEAN NOT NULL,
    error_message TEXT,
    pinged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

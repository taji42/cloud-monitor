import asyncio
import os
import time
from dotenv import load_dotenv
import httpx
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def fetch_active_monitors():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, name, url, timeout_seconds FROM monitors WHERE is_active = TRUE;")
        monitors = cur.fetchall()
    conn.close()
    return monitors

async def send_discord_alert(client, monitor_name, url, status_code, error_message):
    """Sends a formatted alert message to Discord if a target is DOWN."""
    if not DISCORD_WEBHOOK_URL:
        print("[ALERT WARNING] DISCORD_WEBHOOK_URL not found in environment.")
        return

    payload = {
        "embeds": [
            {
                "title": f"⚠️ MONITOR ALERT: {monitor_name} IS DOWN",
                "color": 15158332,  # Red
                "fields": [
                    {"name": "URL", "value": url, "inline": False},
                    {"name": "Status Code", "value": str(status_code or "N/A"), "inline": True},
                    {"name": "Error", "value": str(error_message or "Unknown failure"), "inline": True},
                ],
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
        ]
    }
    
    try:
        response = await client.post(DISCORD_WEBHOOK_URL, json=payload)
        if response.status_code in (200, 204):
            print(f"[ALERT SENT] Discord notification dispatched for {monitor_name}")
        else:
            print(f"[ALERT ERROR] Discord API returned status code {response.status_code}")
    except Exception as e:
        print(f"[ALERT ERROR] Failed to dispatch Discord alert: {e}")

async def ping_target(client, monitor):
    monitor_id = monitor['id']
    name = monitor['name']
    url = monitor['url']
    timeout = monitor['timeout_seconds']
    
    start_time = time.perf_counter()
    status_code = None
    is_up = False
    error_message = None
    
    try:
        response = await client.get(url, timeout=timeout, follow_redirects=True)
        latency_ms = int((time.perf_counter() - start_time) * 1000)
        status_code = response.status_code
        
        if 200 <= status_code < 400:
            is_up = True
        else:
            error_message = f"HTTP Error status code: {status_code}"
            
    except httpx.TimeoutException:
        latency_ms = int((time.perf_counter() - start_time) * 1000)
        error_message = "Request timed out"
    except Exception as e:
        latency_ms = int((time.perf_counter() - start_time) * 1000)
        error_message = str(e)

    print(f"[{'UP' if is_up else 'DOWN'}] {url} | Status: {status_code} | Latency: {latency_ms}ms")

    # Trigger webhook on failure
    if not is_up:
        await send_discord_alert(client, name, url, status_code, error_message)

    return {
        "monitor_id": monitor_id,
        "status_code": status_code,
        "response_time_ms": latency_ms,
        "is_up": is_up,
        "error_message": error_message
    }

def save_ping_logs(logs):
    if not logs:
        return
    conn = get_db_connection()
    with conn.cursor() as cur:
        query = """
            INSERT INTO ping_logs (monitor_id, status_code, response_time_ms, is_up, error_message)
            VALUES (%s, %s, %s, %s, %s);
        """
        records = [
            (log['monitor_id'], log['status_code'], log['response_time_ms'], log['is_up'], log['error_message'])
            for log in logs
        ]
        cur.executemany(query, records)
        conn.commit()
    conn.close()

async def main():
    print("Fetching active monitors...")
    monitors = fetch_active_monitors()
    
    if not monitors:
        print("No active monitors found in database.")
        return

    print(f"Pinging {len(monitors)} target(s) concurrently...")
    
    async with httpx.AsyncClient() as client:
        tasks = [ping_target(client, monitor) for monitor in monitors]
        results = await asyncio.gather(*tasks)

    print("Saving metrics to Supabase...")
    save_ping_logs(results)
    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())

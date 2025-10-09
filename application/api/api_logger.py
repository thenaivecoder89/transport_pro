import logging
from starlette.middleware.base import BaseHTTPMiddleware
import time
from sqlalchemy import create_engine, text
from fastapi import Request
import os
from dotenv import load_dotenv

# Configure logging
logging.getLogger().setLevel(logging.INFO)

def _extract_client_ip(request: Request) -> str:
    # Respect common proxies, then fallback
    xff = (
            request.headers.get("CF-Connecting-IP")
            or request.headers.get("X-Real-IP")
            or request.headers.get("X-Forwarded-For")
    )
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else None


class DBAccessLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        client_ip = _extract_client_ip(request)
        ua = request.headers.get("User-Agent")
        referer = request.headers.get("Referer")

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            # Make sure logging doesn't block the response even if insert fails
            try:
                latency_ms = int(round((time.perf_counter() - start) * 1000))
                # Initialize environment
                load_dotenv()
                DB_URL = os.getenv('RAILWAY_DB_URL')
                # Setup DB Engine
                engine = create_engine(DB_URL, connect_args={'options': '-c search_path="transport_pro"'})
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO api_access_log
                              (method, path, query, status_code, ip_address, user_agent, referer, latency_ms)
                            VALUES
                              (:method, :path, :query, :status_code, :ip_address, :user_agent, :referer, :latency_ms)
                        """),
                        {
                            "method": request.method,
                            "path": request.url.path,
                            "query": request.url.query if request.url.query else None,
                            "status_code": status_code,
                            "ip_address": client_ip,  # PostgreSQL will cast to INET
                            "user_agent": ua,
                            "referer": referer,
                            "latency_ms": latency_ms,
                        }
                    )
            except Exception as e:
                logging.warning(f"[api_access_log] insert failed: {e}")
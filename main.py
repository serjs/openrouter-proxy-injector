import os
import logging
from typing import List, Dict, Optional, AsyncGenerator, Annotated
from contextlib import asynccontextmanager
import pendulum
import httpx
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel
import json
import time
import backoff
import asyncio
import functools
import random

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes the application, API keys, and global HTTP client on startup"""
    global key_status, http_client
    if not OPENROUTER_KEYS:
        logger.error("No OPENROUTER_KEYS provided! Exiting...")
        exit(1)

    key_status = {key: None for key in OPENROUTER_KEYS}
    # max_keepalive_connections: number of idle connections to keep open
    # max_connections: total number of concurrent connections
    limits = httpx.Limits(max_keepalive_connections=20, max_connections=100)
    http_client = httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(300.0))

    logger.info(
        f"Initialized with {len(OPENROUTER_KEYS)} API keys and global HTTP client"
    )

    yield

    """Closes the global HTTP client on application shutdown"""
    if http_client:
        await http_client.aclose()
        logger.info("Global HTTP client closed")


app = FastAPI(lifespan=lifespan)

http_client: Optional[httpx.AsyncClient] = None

# Logging setup
log_level_name = os.getenv("UVICORN_LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("openrouter-proxy")

# Configuration loading
PROXY_API_KEY = os.getenv("PROXY_API_KEY")
if not PROXY_API_KEY:
    logger.error("Error: Environment variable PROXY_API_KEY is not set.")
    exit(1)


def parse_keys_config(config_str: str) -> List[Dict]:
    """Parses the keys configuration string into a list of dictionaries"""
    keys = []
    for item in config_str.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            key, limit = item.split(":", 1)
            try:
                limit = int(limit)
            except ValueError:
                limit = 50
            keys.append({"key": key.strip(), "limit": limit})
        else:
            keys.append({"key": item, "limit": 50})
    return keys


OPENROUTER_CONFIG = parse_keys_config(os.getenv("OPENROUTER_KEYS", ""))
OPENROUTER_KEYS = [k["key"] for k in OPENROUTER_CONFIG]

if not OPENROUTER_KEYS:
    logger.error("Error: Environment variable OPENROUTER_KEYS is not set or empty.")
    exit(1)
TIMEZONE = os.getenv("TIMEZONE", "UTC")

# Initialize key status
key_status: Dict[str, Optional[pendulum.DateTime]] = {}


def non_streaming_retry():
    """Retry decorator for non-streaming requests (network errors only)"""
    return backoff.on_exception(
        backoff.expo,
        (httpx.RequestError, httpx.TimeoutException),
        max_tries=3,
    )


def async_retryable(func):
    """Decorator for retrying streaming requests with key switching logic"""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        last_exception = None
        attempts = 0
        max_attempts = len(OPENROUTER_KEYS) * 2

        while attempts < max_attempts:
            attempts += 1
            selected_key = kwargs.get("selected_key")
            headers = kwargs.get("headers")

            # Check if the key is blocked BEFORE making the request
            current_time = pendulum.now(TIMEZONE)
            lock_time = key_status.get(selected_key)
            is_active = lock_time is None or current_time >= lock_time

            if not is_active:
                new_key = key_manager.get_available_key()
                if new_key:
                    logger.info(f"Switching to new key {new_key[:14]}...")
                    headers["Authorization"] = f"Bearer {new_key}"
                    kwargs["headers"] = headers
                    kwargs["selected_key"] = new_key
                    selected_key = new_key
                else:
                    logger.error("All API keys are rate limited")
                    raise HTTPException(
                        status_code=429,
                        detail="All API keys are rate limited.",
                    )

            try:
                yielded_any = False
                async for value in func(*args, **kwargs):
                    yielded_any = True
                    yield value
                break  # Success
            except (HTTPException, httpx.HTTPStatusError, Exception) as e:
                if yielded_any:
                    logger.warning(
                        f"Error mid-stream, cannot retry: {type(e).__name__}: {e}"
                    )
                    raise e

                status_code = 500
                if isinstance(e, HTTPException):
                    status_code = e.status_code
                elif isinstance(e, httpx.HTTPStatusError):
                    status_code = e.response.status_code

                if status_code in (429, 402):
                    response = (
                        e.response if isinstance(e, httpx.HTTPStatusError) else None
                    )
                    if response:
                        await key_manager.handle_rate_limit_response(
                            selected_key, response
                        )

                    logger.info(
                        f"Key {selected_key[:14]}... {'rate limited' if status_code == 429 else 'payment required'} before stream started, switching..."
                    )

                    new_key = key_manager.get_available_key()
                    if new_key:
                        headers["Authorization"] = f"Bearer {new_key}"
                        kwargs["headers"] = headers
                        kwargs["selected_key"] = new_key

                    last_exception = e
                    await asyncio.sleep(random.uniform(0.1, 0.5))
                else:
                    logger.warning(
                        f"Error before stream started: {type(e).__name__}: {e}. Retrying..."
                    )
                    last_exception = e
                    await asyncio.sleep(random.uniform(0.5, 1.5))
            except (HTTPException, httpx.HTTPStatusError) as e:
                status_code = (
                    e.status_code
                    if isinstance(e, HTTPException)
                    else e.response.status_code
                )
                if status_code in (429, 402):
                    response = None
                    if isinstance(e, httpx.HTTPStatusError):
                        response = e.response
                    if response:
                        await key_manager.handle_rate_limit_response(
                            selected_key, response
                        )

                    logger.info(
                        f"Key {selected_key[:14]}... {'rate limited' if status_code == 429 else 'payment required'}, switching..."
                    )

                    # Force switch to a new key for the next attempt
                    new_key = key_manager.get_available_key()
                    if new_key:
                        headers["Authorization"] = f"Bearer {new_key}"
                        kwargs["headers"] = headers
                        kwargs["selected_key"] = new_key

                    last_exception = e
                    await asyncio.sleep(random.uniform(0.1, 0.5))  # Small jitter
                else:
                    raise
            except Exception as e:
                logger.warning(f"Attempt {attempts} failed: {type(e).__name__}: {e}")
                last_exception = e
                await asyncio.sleep(random.uniform(0.5, 1.5))
        else:
            raise last_exception

    return wrapper


@non_streaming_retry()
async def make_openrouter_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Dict[str, str],
    content: bytes,
    params: Dict[str, str],
    timeout: float,
):
    """Makes a non-streaming request to OpenRouter with retry logic"""
    try:
        response = await client.request(
            method=method,
            url=url,
            headers=headers,
            content=content,
            params=params,
            timeout=timeout,
        )
        return response
    except httpx.RequestError as e:
        logger.warning(f"Request error: {str(e)}")
        return None


def check_retryable_error(response: httpx.Response) -> bool:
    """Checks if the error in the httpx.Response is retryable based on its content"""
    try:
        error_content = response.json()
        if not isinstance(error_content, dict):
            return False
        error_data = error_content.get("error", {})
        error_message = (
            error_data.get("message", "") if isinstance(error_data, dict) else ""
        )
        error_code = (
            error_data.get("code", None) if isinstance(error_data, dict) else None
        )

        if str(error_code) == "429" and "Provider returned error" in error_message:
            return True

    except json.JSONDecodeError:
        logger.warning("Failed to decode JSON from response body during retry check")
    except Exception as e:
        logger.error(f"Unexpected error checking retryable error: {str(e)}")

    return False




def sanitize_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Removes sensitive data from headers"""
    sensitive_keys = ["authorization", "apikey", "cookie", "set-cookie"]
    sanitized = {}
    for k, v in headers.items():
        key_lower = k.lower()
        if key_lower in sensitive_keys:
            sanitized[k] = "[REDACTED]"
        else:
            sanitized[k] = v
    return sanitized


def mask_key(key: Optional[str]) -> str:
    """Masks an API key for logging"""
    if not key:
        return "None"
    return f"{key[:14]}..."


def log_request_debug(
    method: str, url: str, headers: Dict[str, str], body: bytes, prefix: str = ""
):
    """Logs request details in debug mode"""
    sanitized_headers = sanitize_headers(headers)
    body_info = None
    if body:
        try:
            body_info = json.loads(body.decode("utf-8"))
        except:
            body_info = f"Binary data ({len(body)} bytes)"

    log_data = {
        "method": method,
        "url": url,
        "headers": sanitized_headers,
        "body": body_info,
    }

    prefix_str = f"[{prefix}] " if prefix else ""
    logger.debug(
        f"{prefix_str}Request details:\n%s",
        json.dumps(log_data, indent=2, ensure_ascii=False),
    )


def log_response_debug(response: httpx.Response, prefix: str = ""):
    """Logs response details in debug mode"""
    sanitized_headers = sanitize_headers(dict(response.headers))

    log_data = {
        "status_code": response.status_code,
        "headers": sanitized_headers,
    }

    prefix_str = f"[{prefix}] " if prefix else ""
    logger.debug(
        f"{prefix_str}Response details:\n%s",
        json.dumps(log_data, indent=2, ensure_ascii=False),
    )


class KeyManager:
    """Manages OpenRouter API keys, including rotation, rate limiting, and usage tracking"""

    def __init__(self):
        self.current_index = 0
        self.key_configs = {c["key"]: c for c in OPENROUTER_CONFIG}
        self.usage_stats = {
            key: {
                "daily_count": 0,
                "last_request_at": None,
                "reset_at": self._get_next_reset(),
            }
            for key in OPENROUTER_KEYS
        }

    def _get_next_reset(self) -> pendulum.DateTime:
        """Calculates the next daily quota reset time (midnight UTC)"""
        return pendulum.today("UTC").add(days=1)

    def _check_and_reset_quotas(self):
        """Checks if quotas need to be reset based on the current time"""
        now_utc = pendulum.now("UTC")
        for key in OPENROUTER_KEYS:
            if now_utc >= self.usage_stats[key]["reset_at"]:
                logger.info(f"Resetting daily quota for key {key[:14]}...")
                self.usage_stats[key]["daily_count"] = 0
                self.usage_stats[key]["reset_at"] = self._get_next_reset()
                if key_status.get(key) and key_status[key] > pendulum.now(TIMEZONE):
                    # If it was blocked until midnight, clear it
                    key_status[key] = None

    def get_available_key(self) -> Optional[str]:
        """Selects an available API key based on capacity and rate limits"""
        if not OPENROUTER_KEYS:
            return None

        self._check_and_reset_quotas()
        current_time = pendulum.now(TIMEZONE)

        candidates = []
        for key in OPENROUTER_KEYS:
            # 1. Check if blocked by 429/402
            lock_time = key_status.get(key)
            if lock_time and current_time < lock_time:
                continue

            stats = self.usage_stats[key]
            config = self.key_configs[key]

            # 2. Check daily limit
            if stats["daily_count"] >= config["limit"]:
                continue

            # 3. Check RPM (20 requests per minute = 1 request every 3 seconds)
            if stats["last_request_at"]:
                seconds_since_last = (
                    current_time - stats["last_request_at"]
                ).total_seconds()
                if seconds_since_last < 3.0:
                    continue

            # Calculate priority: percentage of remaining quota
            remaining_pct = (config["limit"] - stats["daily_count"]) / config["limit"]
            candidates.append((key, remaining_pct))

        if not candidates:
            return None

        # Sort by remaining percentage (descending) to prioritize keys with more capacity
        candidates.sort(key=lambda x: x[1], reverse=True)

        # Pick from the top candidates to avoid always picking the exact same key if percentages are close
        # This adds a bit of jitter to the round-robin
        top_count = max(1, len(candidates) // 2)
        selected_key = random.choice([c[0] for c in candidates[:top_count]])

        # Update stats immediately to "reserve" the slot
        self.usage_stats[selected_key]["last_request_at"] = current_time
        self.usage_stats[selected_key]["daily_count"] += 1

        logger.debug(
            f"Selected key {selected_key[:14]}... (Usage: {self.usage_stats[selected_key]['daily_count']}/{self.key_configs[selected_key]['limit']})"
        )
        return selected_key

    def block_key_until_next_day(self, key: str):
        """Blocks a key until the next daily reset (e.g., when daily limit is reached)"""
        unlock_time = self._get_next_reset().in_timezone(TIMEZONE)
        key_status[key] = unlock_time

        # Sync internal counter to limit
        if key in self.usage_stats:
            self.usage_stats[key]["daily_count"] = self.key_configs[key]["limit"]

        logger.warning(
            f"Key {key[:14]}... blocked until {unlock_time.to_iso8601_string()} (Daily limit reached)"
        )

    async def handle_rate_limit_response(self, key: str, response: httpx.Response):
        """Handles 429 and 402 responses by blocking keys for appropriate durations"""
        try:
            if response.status_code == 402:
                # Payment Required - block for 5 minutes and warn user
                unlock_time = pendulum.now(TIMEZONE).add(minutes=5)
                key_status[key] = unlock_time
                logger.warning(
                    f"Key {key[:14]}... returned 402 Payment Required. Blocked for 5m. Please check your BYOK credits!"
                )
                return

            try:
                error_content_bytes = await response.aread()
                error_content = json.loads(error_content_bytes.decode("utf-8"))
            except json.decoder.JSONDecodeError:
                error_content = None

            if error_content and isinstance(error_content, dict):
                error_data = error_content.get("error", {})
                error_message = (
                    error_data.get("message", "")
                    if isinstance(error_data, dict)
                    else ""
                )
                error_code = (
                    error_data.get("code", None)
                    if isinstance(error_data, dict)
                    else None
                )

                if str(error_code) == "429":
                    if (
                        "free-models-per-day" in error_message
                        or "Credits exhausted" in error_message
                    ):
                        self.block_key_until_next_day(key)
                        logger.warning(
                            f"Key {key[:14]}... blocked until next day: {error_message}"
                        )
                    elif "Provider returned error" in error_message:
                        # Temporary model-level rate limit from upstream provider
                        unlock_time = pendulum.now(TIMEZONE).add(seconds=30)
                        key_status[key] = unlock_time
                        logger.warning(
                            f"Key {key[:14]}... upstream provider rate-limit. Blocked for 30s."
                        )
                    else:
                        # Other 429 (e.g. OpenRouter's own rate limit for the key)
                        unlock_time = pendulum.now(TIMEZONE).add(seconds=10)
                        key_status[key] = unlock_time
                        logger.warning(
                            f"Key {key[:14]}... OpenRouter rate-limit. Blocked for 10s."
                        )
                else:
                    logger.warning(
                        f"Key {key[:14]}... received error {error_code}: {error_message}"
                    )
            else:
                logger.warning(
                    f"Key {key[:14]}... received status {response.status_code}. Response body is not JSON."
                )

        except Exception as e:
            logger.error(f"Unexpected error handling rate limit response: {str(e)}")

    def get_key_statuses(self) -> Dict[str, Dict]:
        """Returns the current status and usage statistics for all API keys"""
        current_time = pendulum.now(TIMEZONE)
        statuses = {}
        for key in OPENROUTER_KEYS:
            lock_time = key_status.get(key)
            stats = self.usage_stats[key]
            config = self.key_configs[key]

            status_str = "active"
            if lock_time and current_time < lock_time:
                status_str = f"blocked_until_{lock_time.to_iso8601_string()}"
            elif stats["daily_count"] >= config["limit"]:
                status_str = "daily_limit_reached"

            statuses[key] = {
                "status": status_str,
                "usage": f"{stats['daily_count']}/{config['limit']}",
                "remaining": config["limit"] - stats["daily_count"],
                "last_use": (
                    stats["last_request_at"].to_iso8601_string()
                    if stats["last_request_at"]
                    else None
                ),
            }
        return statuses


key_manager = KeyManager()


@app.get("/health")
async def health_endpoint(format: Optional[str] = None):
    """Simple health check endpoint"""
    if format and format.lower() == "json":
        return JSONResponse(content={"status": "OK"}, status_code=200)
    return Response(content="OK", status_code=200)


@app.get("/key-status")
async def get_key_status(apikey: Annotated[str, Header(alias="APIKEY")]):
    """Endpoint to retrieve the status of all managed API keys"""
    if apikey != PROXY_API_KEY:
        logger.warning("Invalid proxy API key for key-status endpoint")
        raise HTTPException(status_code=403, detail="Invalid proxy API key")
    return KeyStatusResponse(keys=key_manager.get_key_statuses())


@async_retryable
async def forward_streaming(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: Dict[str, str],
    content: bytes,
    params: Dict[str, str],
    selected_key: str,
) -> AsyncGenerator[bytes, None]:
    """Asynchronously forwards streaming response with backoff"""
    masked_key_str = mask_key(selected_key)
    try:
        async with client.stream(
            method=method,
            url=url,
            headers=headers,
            content=content,
            params=params,
            timeout=10,
        ) as response:
            log_response_debug(response, prefix=masked_key_str)
            if response.status_code in (429, 402):
                await key_manager.handle_rate_limit_response(selected_key, response)
                raise HTTPException(
                    status_code=response.status_code,
                    detail=(
                        "Rate limited"
                        if response.status_code == 429
                        else "Payment Required"
                    ),
                )

            if response.status_code != 200:
                try:
                    error_body = await response.aread()
                    error_detail = (
                        error_body.decode("utf-8")
                        if error_body
                        else "OpenRouter API error"
                    )
                except Exception as e:
                    error_detail = f"Failed to read error body: {str(e)}"

                logger.error(
                    f"[{masked_key_str}] OpenRouter error: {response.status_code} - {error_detail}"
                )
                raise HTTPException(
                    status_code=response.status_code, detail=error_detail
                )

            # This will be logged in proxy_request
            logger.debug(
                f"[{masked_key_str}] Response headers: {sanitize_headers(dict(response.headers))}"
            )

            async for chunk in response.aiter_bytes():
                yield chunk

    except httpx.HTTPStatusError as e:
        logger.error(f"[{masked_key_str}] HTTP error: {str(e)}")
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except httpx.RequestError as e:
        logger.error(f"[{masked_key_str}] Request failed: {str(e)}")
        raise HTTPException(status_code=500, detail="OpenRouter API unavailable")


@app.api_route(
    "/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
)
async def proxy_request(
    request: Request,
    path: str,
    authorization: Annotated[Optional[str], Header(alias="Authorization")] = None,
    apikey: Annotated[Optional[str], Header(alias="APIKEY")] = None,
):
    """Main proxy endpoint that forwards requests to OpenRouter with key management and retries"""
    # Handle CORS preflight requests locally and don't forward them to OpenRouter
    if request.method == "OPTIONS":
        return Response(
            status_code=200,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, PATCH, OPTIONS",
                "Access-Control-Allow-Headers": "Authorization, APIKEY, Content-Type",
            },
        )

    valid_auth = False
    if apikey and apikey == PROXY_API_KEY:
        valid_auth = True
    elif (
        authorization
        and authorization.startswith("Bearer ")
        and authorization.split(" ")[1] == PROXY_API_KEY
    ):
        valid_auth = True

    if not valid_auth:
        logger.warning("Invalid authentication attempt")
        raise HTTPException(status_code=403, detail="Invalid authentication")

    is_streaming = False
    body_bytes = await request.body()

    # Only allow streaming for specific endpoints and if requested in the body
    # OpenRouter supports streaming only for chat/completions and completions
    if request.method == "POST" and (path.endswith("completions")):
        try:
            if body_bytes:
                request_body = json.loads(body_bytes)
                is_streaming = request_body.get("stream", False)
        except json.JSONDecodeError:
            logger.warning("Failed to parse request body as JSON")

    # Prepare request to OpenRouter
    # openrouter_url = f"https://openrouter.ai/api/v1/{path}"
    # NEW
    if path.startswith("v1/"):
        openrouter_url = f"https://openrouter.ai/api/{path}"
    elif path.startswith("api/v1/"):
        openrouter_url = f"https://openrouter.ai/{path}"
    else:
        openrouter_url = f"https://openrouter.ai/api/v1/{path}"
    params = dict(request.query_params)

    start_time = time.time()

    # For streaming requests, use special handling
    if is_streaming:
        selected_key = key_manager.get_available_key()
        if not selected_key:
            logger.error("All API keys are rate limited")
            raise HTTPException(
                status_code=429,
                detail="All API keys are rate limited. Try after 03:00 UTC.",
            )

        masked_key_str = mask_key(selected_key)
        logger.info(
            f"[{masked_key_str}] Forwarding request to OpenRouter: {request.method} {openrouter_url}"
        )
        logger.debug(f"[{masked_key_str}] Streaming: {is_streaming}")
        logger.debug(f"[{masked_key_str}] Request body: {body_bytes.decode('utf-8')}")

        # Create headers for OpenRouter
        openrouter_headers = {
            "Authorization": f"Bearer {selected_key}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "Origin": "https://openrouter.ai",
            "Referer": "https://openrouter.ai",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0",
        }

        async def streaming_generator():
            nonlocal start_time
            if not http_client:
                logger.error(
                    f"[{mask_key(selected_key)}] Global HTTP client not initialized"
                )
                yield json.dumps(
                    {"error": {"message": "Internal server error", "code": 500}}
                ).encode("utf-8")
                return

            try:
                async for chunk in forward_streaming(
                    client=http_client,
                    method=request.method,
                    url=openrouter_url,
                    headers=openrouter_headers,
                    content=body_bytes,
                    params=params,
                    selected_key=selected_key,
                ):
                    if start_time:
                        duration = time.time() - start_time
                        logger.info(
                            f"[{mask_key(selected_key)}] OpenRouter response: 200 (Streaming started) in {duration:.2f}s"
                        )
                        start_time = None  # Only log once
                    yield chunk
            except HTTPException as e:
                if start_time:
                    duration = time.time() - start_time
                    logger.info(
                        f"[{masked_key_str}] OpenRouter response: {e.status_code} in {duration:.2f}s"
                    )

                error_data = json.dumps(
                    {
                        "error": {
                            "message": e.detail,
                            "type": "api_error",
                            "code": e.status_code,
                        }
                    }
                ).encode("utf-8")
                yield error_data
            except Exception as e:
                if start_time:
                    duration = time.time() - start_time
                    logger.info(
                        f"[{masked_key_str}] OpenRouter response: 500 in {duration:.2f}s"
                    )

                logger.error(f"[{masked_key_str}] Unexpected error: {str(e)}")
                error_data = json.dumps(
                    {
                        "error": {
                            "message": "Internal server error",
                            "type": "server_error",
                            "code": 500,
                        }
                    }
                ).encode("utf-8")
                yield error_data

        return StreamingResponse(
            streaming_generator(),
            media_type="text/event-stream",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Access-Control-Allow-Origin": "*",
            },
        )

    # Handle non-streaming requests
    if not http_client:
        logger.error("Global HTTP client not initialized")
        raise HTTPException(status_code=500, detail="Internal server error")

    try:
        selected_key = key_manager.get_available_key()
        if not selected_key:
            logger.error("All API keys are rate limited")
            raise HTTPException(
                status_code=429,
                detail="All API keys are rate limited.",
            )

        masked_key_str = mask_key(selected_key)
        logger.info(
            f"[{masked_key_str}] Forwarding request to OpenRouter: {request.method} {openrouter_url}"
        )
        logger.debug(f"[{masked_key_str}] Streaming: {is_streaming}")
        logger.debug(f"[{masked_key_str}] Request body: {body_bytes.decode('utf-8')}")

        headers = {
            "Authorization": f"Bearer {selected_key}",
            "X-Title": "OpenrouterProxy",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        response = await make_openrouter_request(
            client=http_client,
            method=request.method,
            url=openrouter_url,
            headers=headers,
            content=body_bytes,
            params=params,
            timeout=300,
        )

        if not response:
            raise HTTPException(status_code=502, detail="OpenRouter request failed")

        log_response_debug(response, prefix=masked_key_str)

        # Handle request limit and retry with different keys if needed
        attempts = 0
        while response.status_code in (429, 402) and attempts < 10:
            attempts += 1
            logger.warning(
                f"[{mask_key(selected_key)}] Received {response.status_code} status. Attempt {attempts}"
            )
            await key_manager.handle_rate_limit_response(selected_key, response)

            selected_key = key_manager.get_available_key()
            if not selected_key:
                break

            masked_key_str = mask_key(selected_key)
            logger.info(f"Retrying with key: {masked_key_str}...")
            headers["Authorization"] = f"Bearer {selected_key}"
            response = await make_openrouter_request(
                client=http_client,
                method=request.method,
                url=openrouter_url,
                headers=headers,
                content=body_bytes,
                params=params,
                timeout=300,
            )
            if not response:
                break
            log_response_debug(response, prefix=masked_key_str)

        if not response:
            raise HTTPException(
                status_code=502, detail="OpenRouter request failed after retries"
            )

        content = response.content

        duration = time.time() - start_time
        logger.info(
            f"[{masked_key_str}] OpenRouter response: {response.status_code} in {duration:.2f}s"
        )
        logger.debug(f"[{masked_key_str}] Response body: {content[:500]}...")

        # Filter out headers that can cause issues with HTTP/2 or are handled by FastAPI
        excluded_headers = {
            "content-encoding",
            "content-length",
            "transfer-encoding",
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailers",
            "upgrade",
        }
        response_headers = {
            k: v
            for k, v in response.headers.items()
            if k.lower() not in excluded_headers
        }
        response_headers["Access-Control-Allow-Origin"] = "*"

        return Response(
            content=content,
            status_code=response.status_code,
            headers=response_headers,
            media_type=response.headers.get("content-type", "application/json"),
        )
    except httpx.RequestError as e:
        duration = time.time() - start_time
        # Note: selected_key might not be defined if get_available_key failed,
        # but we are inside the try block after it succeeded.
        logger.info(
            f"[{masked_key_str}] OpenRouter response: RequestError in {duration:.2f}s"
        )
        logger.error(f"[{masked_key_str}] Request failed: {str(e)}")
        raise HTTPException(status_code=500, detail="OpenRouter API unavailable")


class KeyStatusResponse(BaseModel):
    keys: Dict[str, Dict]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app)

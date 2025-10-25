import json
import os
import random
import threading
import time
from typing import Dict, List, Optional, Tuple

import requests

try:
    from bs4 import BeautifulSoup
except Exception as e:
    raise RuntimeError("beautifulsoup4 is required. pip install beautifulsoup4") from e

# lxml is the preferred parser; ensure installed
try:
    import lxml  # noqa: F401
    DEFAULT_HTML_PARSER = "lxml"
except Exception:
    # Fall back to built-in parser if lxml not available
    DEFAULT_HTML_PARSER = "html.parser"

from .retry import retry


def _env_float(name: str, default: float) -> float:
    try:
        val = os.getenv(name)
        return float(val) if val is not None else default
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        val = os.getenv(name)
        return int(val) if val is not None else default
    except Exception:
        return default


def _env_list(name: str) -> List[str]:
    raw = os.getenv(name)
    if not raw:
        return []
    try:
        # Try JSON first (["ua1","ua2"])
        data = json.loads(raw)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except Exception:
        pass
    # Fallback: comma-separated
    return [x.strip() for x in raw.split(",") if x.strip()]


class TokenBucket:
    """
    Thread-safe token bucket rate limiter.
    rate: tokens per second
    burst: maximum tokens (bucket size)
    """

    def __init__(self, rate: float, burst: int):
        self.rate = max(0.0, rate)
        self.capacity = max(1, burst)
        self.tokens = float(self.capacity)
        self.updated_at = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self):
        now = time.monotonic()
        elapsed = max(0.0, now - self.updated_at)
        self.updated_at = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

    def consume(self, cost: float = 1.0):
        """
        Block until there is at least 'cost' token available, then consume.
        """
        if cost <= 0:
            return
        while True:
            with self._lock:
                self._refill()
                if self.tokens >= cost:
                    self.tokens -= cost
                    return
                # compute time to wait
                needed = cost - self.tokens
                wait_time = needed / self.rate if self.rate > 0 else 0.1
            time.sleep(max(0.0, wait_time))


class UserAgentProvider:
    def __init__(self, ua_env_keys: Tuple[str, ...] = ("CRAWL_USER_AGENTS", "USER_AGENTS"),
                 file_path: str = os.path.join("src", "utils", "ua_list.txt")):
        self._ua_list: List[str] = []
        # Load from env keys
        for key in ua_env_keys:
            self._ua_list.extend(_env_list(key))
        # Load from file if still empty
        if not self._ua_list and os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    self._ua_list = [line.strip() for line in f if line.strip()]
            except Exception:
                pass
        # Fallback minimal UA
        if not self._ua_list:
            self._ua_list = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ]

    def random(self) -> str:
        return random.choice(self._ua_list)


class ProxyProvider:
    """
    Load HTTP proxies from environment:
    - HTTP_PROXIES as JSON list or comma-separated: http://user:pass@host:port, https://host:port
    """

    def __init__(self, env_key: str = "HTTP_PROXIES"):
        self._raw_list = _env_list(env_key)
        self._proxies: List[Dict[str, str]] = []
        for raw in self._raw_list:
            if not raw:
                continue
            # Normalize to requests proxy dict
            self._proxies.append({"http": raw, "https": raw})

    def random(self) -> Optional[Dict[str, str]]:
        if not self._proxies:
            return None
        return random.choice(self._proxies)


# Global configurables
CRAWL_RATE = _env_float("CRAWL_RATE", 1.0)     # requests per second
CRAWL_BURST = _env_int("CRAWL_BURST", 5)       # max burst
PAGE_DELAY_MIN = _env_float("PAGE_DELAY_MIN", 0.1)
PAGE_DELAY_MAX = _env_float("PAGE_DELAY_MAX", 0.5)

_BUCKET = TokenBucket(rate=CRAWL_RATE, burst=CRAWL_BURST)
_UA_PROVIDER = UserAgentProvider()
_PROXY_PROVIDER = ProxyProvider()


def random_page_delay():
    lo = min(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
    hi = max(PAGE_DELAY_MIN, PAGE_DELAY_MAX)
    delay = random.uniform(lo, hi)
    time.sleep(max(0.0, delay))


def _on_backoff(attempt: int, exc: BaseException):
    # Hook for retry decorator: could add metrics or logs if desired
    pass


@retry(
    exceptions=(requests.Timeout, requests.ConnectionError),
    tries=5,
    delay=1,
    backoff=2,
    jitter=0.3,
    timeout=15,
    on_backoff=_on_backoff,
)
def _request_with_anti_scrape(
    method: str,
    url: str,
    *,
    session: Optional[requests.Session] = None,
    params: Optional[Dict] = None,
    data: Optional[Dict] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
    status_retries: int = 3,
) -> requests.Response:
    """
    Core request function guarded by retry decorator for network-level errors.
    Handles:
    - rate limiting (token bucket)
    - randomized page delay
    - rotating UA & proxy
    - status-based backoff for 403/429/503 (rotate UA/proxy and exponential sleep)
    """
    sess = session or requests.Session()
    attempt = 0

    while True:
        attempt += 1

        # Random delay (anti-pattern detection)
        random_page_delay()

        # Rate limit
        _BUCKET.consume(1.0)

        # Rotate UA and possibly proxy every attempt
        ua = _UA_PROVIDER.random()
        proxy = _PROXY_PROVIDER.random()

        req_headers = {"User-Agent": ua}
        if headers:
            req_headers.update(headers)

        try:
            resp = sess.request(
                method=method.upper(),
                url=url,
                params=params,
                data=data,
                headers=req_headers,
                proxies=proxy,
                timeout=timeout,  # provided (or injected by decorator)
            )
        except (requests.Timeout, requests.ConnectionError):
            # Let decorator handle retry
            raise

        if resp.status_code in (403, 429, 503):
            # Status-based backoff; change UA/proxy implicitly by looping
            if attempt > status_retries + 1:
                resp.raise_for_status()
            sleep_for = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            time.sleep(sleep_for)
            continue

        return resp


def get(
    url: str,
    *,
    session: Optional[requests.Session] = None,
    params: Optional[Dict] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
    status_retries: int = 3,
) -> requests.Response:
    """
    Anti-scrape aware GET helper.
    """
    return _request_with_anti_scrape(
        "GET",
        url,
        session=session,
        params=params,
        headers=headers,
        timeout=timeout,
        status_retries=status_retries,
    )


def to_soup(html: str, parser: Optional[str] = None) -> BeautifulSoup:
    return BeautifulSoup(html, parser or DEFAULT_HTML_PARSER)


def parse_json_or_html(
    response: requests.Response,
    *,
    list_selector: Optional[str] = None,
    item_schema: Optional[Dict[str, str]] = None,
) -> Dict:
    """
    Fallback parser: try JSON first; if fails or server returns HTML, parse with BeautifulSoup.

    Parameters:
    - list_selector: CSS selector for a list of items in HTML
    - item_schema: mapping of field -> selector or "attr:<name>" within each item

    Returns a dict:
      {
        "source": "json" | "html",
        "items": [ ...parsed items... ],
        "status_code": <int>,
      }
    """
    ctype = (response.headers.get("Content-Type") or "").lower()
    status = response.status_code

    # Try JSON if content-type or attempt decode
    if "application/json" in ctype or "text/json" in ctype or "charset=json" in ctype:
        try:
            data = response.json()
            if isinstance(data, dict):
                items = data.get("items") or data.get("results") or []
            elif isinstance(data, list):
                items = data
            else:
                items = []
            return {"source": "json", "items": items, "status_code": status}
        except Exception:
            pass

    # HTML fallback
    soup = to_soup(response.text)
    items_out: List[Dict] = []
    if list_selector and item_schema:
        nodes = soup.select(list_selector)
        for node in nodes:
            obj: Dict[str, Optional[str]] = {}
            for field, selector in item_schema.items():
                sel = selector.strip()
                if sel.startswith("attr:"):
                    # attr:href => pick href on the same node
                    attr_name = sel.split(":", 1)[1].strip()
                    obj[field] = node.get(attr_name)
                else:
                    el = node.select_one(sel)
                    if el is None:
                        obj[field] = None
                    elif el.name == "img" and el.get("src"):
                        obj[field] = el.get("src")
                    elif el.has_attr("href"):
                        obj[field] = el.get("href") or el.get_text(strip=True)
                    else:
                        obj[field] = el.get_text(strip=True)
            items_out.append(obj)

    return {"source": "html", "items": items_out, "status_code": status}
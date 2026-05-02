"""
Poll Google Sheets on a schedule (APScheduler cron-style).

When the job runs (default): any row whose publish date/time is **in the past or now**,
and **Publish Status** is not "Posted" (when that column exists), triggers the webhook —
so reels still post if the polling window missed the exact minute. Future rows wait.
Set **STRICT_TIME_MATCH=1** for the old same-day + time-window behavior. Webhooks are
deduped in-process per row+schedule until you mark Posted or restart.

Setup (pick one auth method):

  A) Service account JSON (if your org allows creating keys):
     Enable Sheets API, share the sheet with the service account email. Prefer credentials
     in .env: set **GOOGLE_SERVICE_ACCOUNT_JSON** to the full key JSON (single line is
     easiest; keep \\n inside the private_key string). Optional: **GOOGLE_SERVICE_ACCOUNT_JSON_B64**
     (base64 of the UTF-8 JSON file). Fallback: **GOOGLE_SERVICE_ACCOUNT_FILE** /
     **GOOGLE_APPLICATION_CREDENTIALS** pointing to a file, or DEFAULT_SERVICE_ACCOUNT_JSON
     next to this script.

  B) No JSON keys allowed (org policy iam.disableServiceAccountKeyCreation):
     Install Google Cloud SDK, run: gcloud auth application-default login
     Your normal Google account must have access to the spreadsheet (open it once in the
     browser). ADC is stored on your machine; no service account key file.

  Optional: WEBHOOK_URL, WEBHOOK_HTTP_METHOD (POST or GET — see n8n notes below),
  TIMEZONE, CHECK_INTERVAL_MINUTES, STRICT_TIME_MATCH, CATCH_UP_MAX_PAST_DAYS,
  MATCH_WINDOW_MINUTES, MATCH_PRECISION, columns.

  n8n: If you see "webhook is not registered for POST", either use the Production URL with
  POST (default) or set WEBHOOK_HTTP_METHOD=GET for test URLs (only query params, not full row JSON).

Run (long-running scheduler, default: every CHECK_INTERVAL_MINUTES):
  python sheet_cron.py

Or run once (no scheduler), useful from system cron / Task Scheduler:
  python sheet_cron.py --once
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo, available_timezones

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from google.auth import default as google_auth_default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2.service_account import Credentials
import gspread

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Avoid posting the same row+schedule twice in one long-running process (sheet status may lag).
_WEBHOOK_SENT: set[str] = set()

# https://docs.google.com/spreadsheets/d/1jGLndAUDcYShoATkYNIRzFMsft8rwqN_wLmhw7BZnMA/edit?gid=238208381 — override with SPREADSHEET_ID / WORKSHEET_NAME
DEFAULT_SPREADSHEET_ID = "1jGLndAUDcYShoATkYNIRzFMsft8rwqN_wLmhw7BZnMA"
# Tab gid=238208381 → name must match exactly (case-sensitive in API).
DEFAULT_WORKSHEET_NAME = "Sheet3"
DEFAULT_WEBHOOK_URL = "https://finzarc.app.n8n.cloud/webhook/cron"

# Fallback service account filename next to this script if env / GOOGLE_* file vars unset.
DEFAULT_SERVICE_ACCOUNT_JSON = "psyched-equator-495111-e1-e33b35262ad1.json"

SCOPES = ("https://www.googleapis.com/auth/spreadsheets.readonly",)

DATE_FORMATS = ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%d/%m/%Y")
TIME_FORMATS = ("%I:%M:%S %p", "%I:%M:%S%p", "%I:%M %p", "%H:%M:%S", "%H:%M")


def _script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _service_account_info_from_env() -> dict[str, Any] | None:
    """
    Load service-account dict from GOOGLE_SERVICE_ACCOUNT_JSON or
    GOOGLE_SERVICE_ACCOUNT_JSON_B64; otherwise None.
    """
    raw = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if raw:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(
                "GOOGLE_SERVICE_ACCOUNT_JSON is set but is not valid JSON. "
                "Use one line (minified) or quoted multi-line; in private_key use \\n for newlines."
            ) from e
        return data if isinstance(data, dict) else None

    b64 = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64") or "").strip()
    if not b64:
        return None
    try:
        raw = base64.b64decode(b64).decode("utf-8")
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(
            "GOOGLE_SERVICE_ACCOUNT_JSON_B64 is set but could not be decoded as base64 UTF-8 JSON."
        ) from e
    return data if isinstance(data, dict) else None


def _service_account_info() -> dict[str, Any] | None:
    """Env JSON first, then a key file on disk if present."""
    info = _service_account_info_from_env()
    if info is not None:
        return info
    p = _find_service_account_json_path()
    if not p:
        return None
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _find_service_account_json_path() -> str | None:
    """Return path to a service account key file if present; otherwise None."""
    _here = _script_dir()
    candidates: list[str] = []

    def _push_env_path(raw: str) -> None:
        exp = os.path.expanduser(raw.strip())
        if os.path.isabs(exp):
            candidates.append(os.path.normpath(exp))
        else:
            # Relative paths: try next to this script first (stable), then CWD
            candidates.append(os.path.normpath(os.path.join(_here, exp)))
            candidates.append(os.path.normpath(os.path.join(os.getcwd(), exp)))

    for key in ("GOOGLE_SERVICE_ACCOUNT_FILE", "GOOGLE_APPLICATION_CREDENTIALS"):
        v = (os.getenv(key) or "").strip()
        if v:
            _push_env_path(v)

    candidates.append(os.path.join(_here, DEFAULT_SERVICE_ACCOUNT_JSON))
    for name in (
        "google-service-account.json",
        "service-account.json",
        "google_credentials.json",
    ):
        candidates.append(os.path.join(_here, name))

    seen: set[str] = set()
    for path in candidates:
        if not path:
            continue
        norm = os.path.normcase(os.path.normpath(path))
        if norm in seen:
            continue
        seen.add(norm)
        if os.path.isfile(path):
            ap = os.path.abspath(os.path.normpath(path))
            logger.info("Google Sheets auth: using %s", ap)
            return ap

    logger.warning(
        "No service account JSON on disk (checked script dir %s for %s among others).",
        _here,
        DEFAULT_SERVICE_ACCOUNT_JSON,
    )
    return None


def _get_sheets_credentials():
    """
    Prefer GOOGLE_SERVICE_ACCOUNT_JSON / _B64, then a service account JSON file on disk.
    Otherwise use Application Default Credentials (user login). Use this when your
    organization disables service account key creation.
    """
    info = _service_account_info_from_env()
    if info:
        logger.info("Google Sheets auth: using GOOGLE_SERVICE_ACCOUNT_JSON (or _B64) from environment")
        return Credentials.from_service_account_info(info, scopes=SCOPES)

    key_path = _find_service_account_json_path()
    if key_path:
        return Credentials.from_service_account_file(key_path, scopes=SCOPES)

    try:
        creds, _project = google_auth_default(scopes=list(SCOPES))
        return creds
    except DefaultCredentialsError as e:
        raise FileNotFoundError(
            "No Google credentials found.\n\n"
            "If your organization blocks service account keys "
            "(iam.disableServiceAccountKeyCreation), use Application Default Credentials:\n"
            "  1. Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install\n"
            "  2. Open a new terminal and run: gcloud auth application-default login\n"
            "  3. Sign in with the Google account that can open this spreadsheet.\n\n"
            "If keys are allowed, set GOOGLE_SERVICE_ACCOUNT_JSON in .env, or "
            "GOOGLE_SERVICE_ACCOUNT_FILE to your JSON path, or place "
            "google-service-account.json in:\n"
            f"  {_script_dir()}\n\n"
            f"Underlying error: {e}"
        ) from e


def _get_timezone() -> ZoneInfo | Any:
    """IANA name via TIMEZONE, or system local zone."""
    name = (os.getenv("TIMEZONE") or "").strip()
    if name:
        if name not in available_timezones():
            raise ValueError(f"Unknown TIMEZONE={name!r}. Use an IANA name, e.g. America/New_York.")
        return ZoneInfo(name)
    now = datetime.now().astimezone()
    tz = now.tzinfo
    if tz is None:
        return ZoneInfo("UTC")
    return tz


def _norm_header(h: str) -> str:
    return " ".join(h.strip().lower().split())


def _row_dict(header: list[str], row: list[Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for i, key in enumerate(header):
        if not key or not str(key).strip():
            continue
        val = row[i] if i < len(row) else ""
        out[str(key).strip()] = val if isinstance(val, str) else str(val)
    return out


def _parse_date_only(raw: str) -> date | None:
    s = raw.strip()
    if not s:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_time_only(raw: str) -> time | None:
    s = raw.strip()
    if not s:
        return None
    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None


def _parse_publish_at(
    date_str: str,
    time_str: str,
    tz: ZoneInfo | Any,
) -> datetime | None:
    d = _parse_date_only(date_str)
    t = _parse_time_only(time_str)
    if d is None or t is None:
        return None
    return datetime.combine(d, t, tzinfo=tz)


def _match_precision() -> str:
    p = (os.getenv("MATCH_PRECISION") or "minute").strip().lower()
    if p in ("minute", "second"):
        return p
    return "minute"


def _check_interval_minutes() -> int:
    return max(1, int(os.getenv("CHECK_INTERVAL_MINUTES", "30")))


def _match_window_minutes() -> int | None:
    """
    Used only when STRICT_TIME_MATCH=1. Window: same day and late by 0..N minutes.
    If exact / strict / 0: require exact minute or second (MATCH_PRECISION) instead.
    """
    raw = (os.getenv("MATCH_WINDOW_MINUTES") or "").strip().lower()
    if raw in ("exact", "strict", "0"):
        return None
    if raw != "":
        try:
            v = int(raw)
            return None if v <= 0 else v
        except ValueError:
            pass
    return _check_interval_minutes()


def _strict_time_match_enabled() -> bool:
    """If true, use same-day + time window or exact clock (old behavior)."""
    return os.getenv("STRICT_TIME_MATCH", "0").strip().lower() in ("1", "true", "yes", "on")


def _strict_schedule_match(publish_at: datetime, now: datetime) -> bool:
    """Same calendar date as `now`, then window lateness or exact clock match."""
    if publish_at.tzinfo != now.tzinfo:
        publish_at = publish_at.astimezone(now.tzinfo)

    if publish_at.date() != now.date():
        return False

    window = _match_window_minutes()
    if window is not None:
        late_sec = (now - publish_at).total_seconds()
        return 0 <= late_sec <= window * 60

    prec = _match_precision()
    if prec == "second":
        return (
            publish_at.hour == now.hour
            and publish_at.minute == now.minute
            and publish_at.second == now.second
        )

    return publish_at.hour == now.hour and publish_at.minute == now.minute


def _should_trigger_webhook(
    publish_at: datetime,
    now: datetime,
    publish_status: str,
    has_status_col: bool,
) -> bool:
    """
    Default (STRICT_TIME_MATCH=0): fire if publish time is in the past (or now) and the
    row is not marked Published — so missed windows still post. Future rows are skipped.

    Set STRICT_TIME_MATCH=1 to require same-day + narrow time window or exact clock.
    """
    if publish_at.tzinfo != now.tzinfo:
        publish_at = publish_at.astimezone(now.tzinfo)

    if has_status_col and _should_skip_by_status(publish_status):
        return False

    if _strict_time_match_enabled():
        return _strict_schedule_match(publish_at, now)

    if publish_at > now:
        return False

    cap = (os.getenv("CATCH_UP_MAX_PAST_DAYS") or "").strip()
    if cap:
        try:
            oldest = now - timedelta(days=int(cap))
            if publish_at < oldest:
                return False
        except ValueError:
            pass
    return True


def _col_keys() -> tuple[str, str, str | None]:
    return (
        os.getenv("COL_PUBLISH_DATE", "Publish Date").strip(),
        os.getenv("COL_PUBLISH_TIME", "Publish Time").strip(),
        (os.getenv("COL_PUBLISH_STATUS") or "Publish Status").strip() or None,
    )


def _header_index_map(header: list[str]) -> dict[str, int]:
    return {_norm_header(h): i for i, h in enumerate(header) if h and str(h).strip()}


def _get_cell(row: list[Any], header: list[str], col_name: str) -> str:
    by_norm = _header_index_map(header)
    idx = by_norm.get(_norm_header(col_name))
    if idx is None:
        return ""
    if idx >= len(row):
        return ""
    v = row[idx]
    return v if isinstance(v, str) else str(v)


def _open_worksheet(sh: Any, title: str) -> Any:
    """
    Open a tab by exact title, then case-insensitive match. Google Sheet tab names
    are case-sensitive (e.g. Sheet3 vs sheet3).
    """
    t = title.strip()
    if not t:
        raise ValueError("WORKSHEET_NAME is empty.")
    try:
        return sh.worksheet(t)
    except gspread.exceptions.WorksheetNotFound:
        pass
    t_lower = t.lower()
    for ws in sh.worksheets():
        if ws.title.strip().lower() == t_lower:
            return ws
    available = [w.title for w in sh.worksheets()]
    raise ValueError(
        f"Worksheet {t!r} not found. Set WORKSHEET_NAME in .env to the exact tab name. "
        f"Available tabs: {available!r}"
    )


def _google_sheet_403_hint() -> str:
    """Explain 403 and surface service-account email when possible."""
    data = _service_account_info()
    if data:
        email = data.get("client_email")
        if isinstance(email, str) and email.strip():
            return (
                " In Google Sheets → Share, add this Google account with at least Viewer: "
                f"{email.strip()}"
            )
        if data.get("type") != "service_account":
            return (
                " Your JSON may not be a service-account key (expect type=service_account). "
                "Use a Sheets API service account + Share that email, or use ADC."
            )
    return (
        " Share the spreadsheet with the identity your credentials use "
        "(service account client_email from the JSON, or the Google account used for ADC)."
    )


def get_sheet_rows() -> list[list[Any]]:
    spreadsheet_id = os.getenv("SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID)
    worksheet_name = os.getenv("WORKSHEET_NAME", DEFAULT_WORKSHEET_NAME)

    creds = _get_sheets_credentials()
    gc = gspread.authorize(creds)
    try:
        sh = gc.open_by_key(spreadsheet_id)
    except PermissionError:
        logger.error(
            "Google Sheets API 403 — caller cannot access spreadsheet %s.%s",
            spreadsheet_id,
            _google_sheet_403_hint(),
        )
        raise
    ws = _open_worksheet(sh, worksheet_name)
    return ws.get_all_values()


def _call_webhook(url: str, payload: dict[str, Any]) -> tuple[int, str]:
    """
    POST JSON by default. Set WEBHOOK_HTTP_METHOD=GET for n8n *test* URLs that only allow GET
    (Production webhook URLs in n8n usually accept POST with the full JSON body).
    GET sends compact query params (sheet_row, publish_at); full row is not in the URL.
    """
    timeout = int(os.getenv("WEBHOOK_TIMEOUT_SEC", "60"))
    method = (os.getenv("WEBHOOK_HTTP_METHOD") or "POST").strip().upper()

    if method == "GET":
        params = {
            "sheet_row": str(payload.get("sheet_row", "")),
            "publish_at": str(payload.get("publish_at", "")),
            "matched_at": str(payload.get("matched_at", "")),
            "source": str(payload.get("source", "sheet_cron")),
        }
        row = payload.get("row")
        if isinstance(row, dict) and row.get("ID"):
            params["id"] = str(row["ID"])
        q = urllib.parse.urlencode(params)
        sep = "&" if "?" in url else "?"
        full_url = f"{url}{sep}{q}"
        req = urllib.request.Request(
            full_url,
            headers={"Accept": "application/json"},
            method="GET",
        )
    else:
        data = json.dumps(payload, default=str).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )

    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return resp.getcode(), body


def _should_skip_by_status(publish_status: str) -> bool:
    """
    True when Publish Status equals PUBLISHED_STATUS_VALUE (default 'Posted').
    Always enforced — rows marked posted never trigger the webhook.
    """
    mark = (os.getenv("PUBLISHED_STATUS_VALUE") or "Posted").strip()
    if not mark:
        return False
    if not publish_status or not publish_status.strip():
        return False
    return publish_status.strip().lower() == mark.lower()


def process_sheet_rows(rows: list[list[Any]]) -> int:
    """Run matching + webhooks. Returns how many webhooks were sent successfully."""
    if not rows:
        logger.info("Sheet is empty.")
        return 0

    header = [str(c).strip() for c in rows[0]]
    col_date, col_time, col_status = _col_keys()
    tz = _get_timezone()
    now = datetime.now(tz)
    webhook_url = (os.getenv("WEBHOOK_URL") or DEFAULT_WEBHOOK_URL).strip()

    if not any(c for c in header if c):
        logger.warning("Header row is empty.")
        return 0

    win = _match_window_minutes()
    mode = "strict_time" if _strict_time_match_enabled() else "catch_up_past_due"
    logger.info(
        "now=%s %s (%s) mode=%s window=%s precision=%s",
        now.date().isoformat(),
        now.strftime("%H:%M:%S"),
        getattr(tz, "key", None) or str(tz),
        mode,
        win if win is not None else "exact",
        _match_precision(),
    )

    matches = 0
    for row_index, row in enumerate(rows[1:], start=2):
        if not any(str(c).strip() for c in row if c is not None):
            continue

        date_s = _get_cell(row, header, col_date)
        time_s = _get_cell(row, header, col_time)
        if not date_s or not time_s:
            continue

        publish_at = _parse_publish_at(date_s, time_s, tz)
        if publish_at is None:
            logger.warning("Row %s: bad date/time %r / %r", row_index, date_s, time_s)
            continue

        logger.info(
            "Row %s sheet publish date=%r time=%r → parsed %s %s",
            row_index,
            date_s,
            time_s,
            publish_at.date().isoformat(),
            publish_at.strftime("%H:%M:%S"),
        )

        status_s = _get_cell(row, header, col_status) if col_status else ""
        if not _should_trigger_webhook(publish_at, now, status_s, bool(col_status)):
            continue

        dedupe_key = f"{row_index}|{publish_at.isoformat(timespec='minutes')}"
        if dedupe_key in _WEBHOOK_SENT:
            continue

        payload = {
            "source": "sheet_cron",
            "sheet_row": row_index,
            "matched_at": now.isoformat(),
            "publish_at": publish_at.isoformat(),
            "catch_up": not _strict_time_match_enabled() and publish_at < now,
            "row": _row_dict(header, row),
        }
        try:
            code, body = _call_webhook(webhook_url, payload)
            matches += 1
            _WEBHOOK_SENT.add(dedupe_key)
            logger.info("Row %s webhook http=%s", row_index, code)
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
            logger.error(
                "Row %s: webhook HTTP %s: %s",
                row_index,
                e.code,
                err_body[:500],
            )
        except urllib.error.URLError as e:
            logger.error("Row %s: webhook URL error: %s", row_index, e)

    if matches == 0:
        logger.info("No matches.")
    return matches


def job_read_sheet() -> None:
    try:
        rows = get_sheet_rows()
    except Exception:
        logger.exception("Failed to read Google Sheet")
        return
    process_sheet_rows(rows)


def run_sheet_cron_once() -> int:
    """
    Read the sheet and run one check (same as `python sheet_cron.py --once`).
    Returns the number of successful webhook calls. Raises on Google Sheet / auth errors.
    """
    rows = get_sheet_rows()
    return process_sheet_rows(rows)


def _cron_trigger_legacy() -> CronTrigger:
    """Optional SCHEDULE_KIND=cron: old cron-style triggers."""
    if os.getenv("RUN_EVERY_MINUTE", "0").strip().lower() in ("1", "true", "yes", "on"):
        if _match_precision() == "second":
            return CronTrigger(second="*")
        return CronTrigger(second=0, minute="*")

    minute = int(os.getenv("SCHEDULE_MINUTE", "0"))
    hour = int(os.getenv("SCHEDULE_HOUR", "9"))
    kwargs: dict[str, int | str] = {"minute": minute, "hour": hour}
    for field, env_name in (
        ("day", "SCHEDULE_DAY"),
        ("month", "SCHEDULE_MONTH"),
        ("day_of_week", "SCHEDULE_DAY_OF_WEEK"),
    ):
        raw = os.getenv(env_name, "*").strip()
        if raw != "*":
            kwargs[field] = int(raw) if raw.isdigit() else raw
    return CronTrigger(**kwargs)


def _scheduler_trigger_from_env() -> IntervalTrigger | CronTrigger:
    """
    Default: repeat every CHECK_INTERVAL_MINUTES (30 by default) to poll the sheet.
    Set SCHEDULE_KIND=cron for legacy cron triggers (RUN_EVERY_MINUTE, SCHEDULE_HOUR, …).
    """
    kind = (os.getenv("SCHEDULE_KIND") or "interval").strip().lower()
    if kind in ("cron", "legacy"):
        return _cron_trigger_legacy()

    mins = _check_interval_minutes()
    mins = min(mins, 24 * 60)
    return IntervalTrigger(minutes=mins)


def main() -> None:
    parser = argparse.ArgumentParser(description="Google Sheet reader with APScheduler.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single read and exit (use with system cron).",
    )
    args = parser.parse_args()

    if args.once:
        try:
            run_sheet_cron_once()
        except Exception:
            logger.exception("Failed to read Google Sheet")
            sys.exit(1)
        return

    trigger = _scheduler_trigger_from_env()
    scheduler = BlockingScheduler()
    scheduler.add_job(
        job_read_sheet,
        trigger=trigger,
        id="read_google_sheet",
        replace_existing=True,
    )
    logger.info(
        "Scheduler started: %s (CHECK_INTERVAL_MINUTES / SCHEDULE_KIND — see docstring). Ctrl+C to stop.",
        trigger,
    )

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()

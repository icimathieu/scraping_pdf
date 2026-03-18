import argparse
import csv
import http.cookiejar
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    InvalidCookieDomainException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

NON_ALNUM_PATTERN = re.compile(r"[^a-zA-Z0-9_.-]+")
ARK_PATTERN = re.compile(r"(ark:/12148/[a-z0-9]+)", re.IGNORECASE)


class CaptchaRequiredError(Exception):
    """Raised when Gallica serves ALTCHA instead of the requested PDF."""


class TooManyRequestsError(Exception):
    """Raised when Gallica serves a 429/rate-limit page."""


class ThrottleStopError(RuntimeError):
    """Raised when repeated throttling indicates the script should stop entirely."""


class RateLimiter:
    def __init__(self, requests_per_minute: float) -> None:
        self.requests_per_minute = max(0.01, float(requests_per_minute))
        self.min_interval_seconds = 60.0 / self.requests_per_minute
        self.next_allowed_at = 0.0

    def wait_turn(self) -> None:
        now = time.monotonic()
        if now < self.next_allowed_at:
            sleep_for = self.next_allowed_at - now
            log(
                f"[INFO][rate_limiter] Sleeping {sleep_for:.1f}s "
                f"(rpm={self.requests_per_minute:.3f})"
            )
            time.sleep(sleep_for)
        self.next_allowed_at = time.monotonic() + self.min_interval_seconds


class CircuitBreaker429:
    def __init__(self, threshold: int, sleep_seconds: int, max_cooldowns: int) -> None:
        self.threshold = max(1, threshold)
        self.sleep_seconds = max(1, sleep_seconds)
        self.max_cooldowns = max(1, max_cooldowns)
        self.consecutive_429 = 0
        self.cooldowns_used = 0

    def record_success(self) -> None:
        if self.consecutive_429 > 0:
            log(
                f"[INFO][circuit_breaker] reset consecutive_429={self.consecutive_429} after success"
            )
        self.consecutive_429 = 0

    def record_failure(self, exc: Exception, context: str) -> None:
        if not isinstance(exc, (TooManyRequestsError, CaptchaRequiredError)):
            self.consecutive_429 = 0
            return
        self.consecutive_429 += 1
        log(
            f"[WARN][circuit_breaker] throttle streak={self.consecutive_429}/{self.threshold} "
            f"context={context}"
        )
        if self.consecutive_429 >= self.threshold:
            if self.cooldowns_used >= self.max_cooldowns:
                raise ThrottleStopError(
                    "throttle_stop: trop de signaux de throttling malgre les cooldowns; arret definitif"
                )
            log(
                f"[WARN][circuit_breaker] Sleeping {self.sleep_seconds}s after "
                f"{self.consecutive_429} consecutive throttle signals"
            )
            time.sleep(self.sleep_seconds)
            self.cooldowns_used += 1
            self.consecutive_429 = 0


def log(message: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {message}", flush=True)


def sanitize_path_part(value: str, fallback: str) -> str:
    clean = NON_ALNUM_PATTERN.sub("_", value).strip("._")
    return clean if clean else fallback


def normalize_issue_ark(value: str) -> str:
    match = ARK_PATTERN.search(value)
    if not match:
        raise ValueError(f"ARK introuvable pour le numero: {value}")
    return match.group(1)


def error_code_from_exception(exc: Exception) -> str:
    if isinstance(exc, CaptchaRequiredError):
        return "captcha_required"
    if isinstance(exc, TooManyRequestsError):
        return "429"
    if isinstance(exc, TimeoutError):
        return "timeout"
    if isinstance(exc, TimeoutException):
        return "timeout"
    if isinstance(exc, InvalidCookieDomainException):
        return "invalid_cookie_domain"
    if isinstance(exc, WebDriverException):
        return "webdriver_error"
    if isinstance(exc, FileNotFoundError):
        return "file_not_found"
    if isinstance(exc, OSError):
        return "os_error"
    if isinstance(exc, ValueError):
        return "value_error"
    return exc.__class__.__name__


def is_pdf_signature(content_prefix: bytes) -> bool:
    candidate = content_prefix.lstrip()
    if candidate.startswith(b"\xef\xbb\xbf"):
        candidate = candidate[3:]
    return candidate.startswith(b"%PDF")


def load_payload(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("Le fichier d'entree doit contenir un objet JSON.")
    return payload


def ensure_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = payload.get("items")
    if isinstance(items, list):
        return items
    raise ValueError("Le fichier d'entree doit contenir une liste 'items'.")


def save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def save_csv(path: Path, items: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "revue",
        "parent_ark_date",
        "year",
        "day_of_year",
        "numero_id",
        "issue_ark",
        "precision",
        "pdf_url",
        "pdf_path",
        "pdf_size_bytes",
        "status",
        "pipeline_status",
        "error_stage",
        "error_code",
        "error_message",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            writer.writerow({key: item.get(key, "") for key in fieldnames})


def cleanup_empty_dirs(start_dir: Path, stop_dir: Path) -> None:
    current = start_dir
    try:
        stop_resolved = stop_dir.resolve()
    except Exception:
        stop_resolved = stop_dir
    while True:
        if not current.exists():
            break
        try:
            current_resolved = current.resolve()
        except Exception:
            current_resolved = current
        if current_resolved == stop_resolved:
            break
        try:
            current.rmdir()
        except OSError:
            break
        parent = current.parent
        if parent == current:
            break
        current = parent


def build_firefox_options(
    download_dir: Path,
    headless: bool,
    firefox_binary: Path,
    user_agent: str,
) -> Options:
    options = Options()
    if headless:
        options.add_argument("-headless")
    options.binary_location = str(firefox_binary)
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.dir", str(download_dir))
    options.set_preference("browser.download.useDownloadDir", True)
    options.set_preference(
        "browser.helperApps.neverAsk.saveToDisk",
        ",".join(
            [
                "application/pdf",
                "application/octet-stream",
                "application/x-pdf",
                "binary/octet-stream",
            ]
        ),
    )
    options.set_preference("pdfjs.disabled", True)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.alwaysOpenPanel", False)
    if user_agent.strip():
        options.set_preference("general.useragent.override", user_agent.strip())
    return options


def load_cookies(cookies_file: str) -> List[http.cookiejar.Cookie]:
    if not cookies_file:
        return []
    cookie_path = Path(cookies_file)
    if not cookie_path.exists():
        log(f"[WARN][pdf] Fichier cookies introuvable: {cookie_path}")
        return []
    jar = http.cookiejar.MozillaCookieJar()
    jar.load(str(cookie_path), ignore_discard=True, ignore_expires=True)
    return list(jar)


def seed_driver_cookies(
    driver: webdriver.Firefox,
    base_url: str,
    cookies_file: str,
) -> int:
    cookies = load_cookies(cookies_file)
    if not cookies:
        return 0
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"URL invalide pour l'initialisation des cookies: {base_url}")
    origin = f"{parsed.scheme}://{parsed.netloc}/"
    driver.get(origin)
    added = 0
    for cookie in cookies:
        domain = str(cookie.domain or "").lstrip(".")
        if domain and domain not in parsed.netloc:
            continue
        selenium_cookie = {
            "name": cookie.name,
            "value": cookie.value,
            "path": cookie.path or "/",
        }
        if cookie.domain:
            selenium_cookie["domain"] = cookie.domain
        if cookie.secure:
            selenium_cookie["secure"] = True
        if cookie.expires:
            selenium_cookie["expiry"] = int(cookie.expires)
        try:
            driver.add_cookie(selenium_cookie)
            added += 1
        except InvalidCookieDomainException:
            continue
    log(f"[INFO][pdf] Cookies charges dans Firefox: {added}")
    driver.get("about:blank")
    return added


def resolve_click_locator(args: argparse.Namespace) -> Optional[Tuple[str, str]]:
    if args.click_css_selector:
        return (By.CSS_SELECTOR, args.click_css_selector)
    if args.click_id_prefix:
        return (By.CSS_SELECTOR, f"[id^='{args.click_id_prefix}']")
    if args.click_xpath:
        return (By.XPATH, args.click_xpath)
    return None


def click_download_trigger(
    driver: webdriver.Firefox,
    locator: Tuple[str, str],
    timeout_seconds: int,
) -> None:
    element = WebDriverWait(driver, timeout_seconds).until(EC.element_to_be_clickable(locator))
    driver.execute_script(
        "arguments[0].scrollIntoView({block: 'center', inline: 'center'});",
        element,
    )
    try:
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)


def list_download_candidates(download_dir: Path) -> Set[Path]:
    return {
        path
        for path in download_dir.iterdir()
        if path.is_file() and not path.name.endswith(".part")
    }


def cleanup_new_downloads(download_dir: Path, existing_files: Iterable[Path]) -> None:
    existing_set = set(existing_files)
    for path in download_dir.iterdir():
        if path.is_file() and path not in existing_set:
            try:
                size = path.stat().st_size
                if size > 0:
                    with path.open("rb") as fh:
                        prefix = fh.read(8192)
                    if is_pdf_signature(prefix):
                        log(
                            f"[INFO][pdf][cleanup] Conservation du PDF telecharge: {path.name}"
                        )
                        continue
            except OSError:
                pass
            try:
                path.unlink()
            except OSError:
                pass


def infer_filename_from_url(pdf_url: str) -> str:
    parsed = urlparse(pdf_url)
    name = Path(parsed.path).name
    return name or "download.pdf"


def inspect_download_payload(path: Path) -> Tuple[int, bytes]:
    with path.open("rb") as fh:
        prefix = fh.read(8192)
    return path.stat().st_size, prefix


def raise_for_non_pdf_payload(path: Path) -> None:
    size_bytes, prefix = inspect_download_payload(path)
    if size_bytes <= 0:
        raise ValueError("empty_payload")
    if is_pdf_signature(prefix):
        return
    lowered = prefix.lstrip().lower()
    text = lowered.decode("utf-8", errors="ignore")
    if "altcha" in text:
        raise CaptchaRequiredError("captcha_required:payload_altcha")
    if "too many requests" in text or " 429" in text or text.startswith("429"):
        raise TooManyRequestsError("429:payload_page")
    if lowered.startswith(b"<!doctype html") or lowered.startswith(b"<html"):
        raise ValueError("non_pdf_html_payload")
    raise ValueError("non_pdf_signature")


def infer_failure_from_browser_state(driver: webdriver.Firefox) -> Exception:
    current_url = ""
    title = ""
    source = ""
    try:
        current_url = str(driver.current_url or "")
    except Exception:
        current_url = ""
    try:
        title = str(driver.title or "")
    except Exception:
        title = ""
    try:
        source = str(driver.page_source or "")
    except Exception:
        source = ""

    haystack = " ".join([current_url.lower(), title.lower(), source.lower()])
    if "altcha" in haystack:
        return CaptchaRequiredError(f"captcha_required:browser_state:{current_url or title}")
    if "429" in haystack or "too many requests" in haystack:
        return TooManyRequestsError(f"429:browser_state:{current_url or title}")
    if source.strip():
        return ValueError(f"download_not_detected:url={current_url or 'unknown'}")
    return TimeoutError("telechargement_non_detecte")


def detect_429_from_browser_state(driver: webdriver.Firefox) -> Optional[Exception]:
    current_url = ""
    title = ""
    source = ""
    try:
        current_url = str(driver.current_url or "")
    except Exception:
        current_url = ""
    try:
        title = str(driver.title or "")
    except Exception:
        title = ""
    try:
        source = str(driver.page_source or "")
    except Exception:
        source = ""

    title_l = title.lower()
    source_l = source.lower()
    text_haystack = " ".join([title_l, source_l])

    explicit_markers = (
        "too many requests",
        "http 429",
        "http error 429",
        "status code 429",
        "error 429",
        "erreur 429",
    )
    if any(marker in text_haystack for marker in explicit_markers):
        return TooManyRequestsError(f"429:browser_state:{current_url or title}")

    if re.search(r"\b429\b", text_haystack) and (
        "request" in text_haystack
        or "requete" in text_haystack
        or "rate limit" in text_haystack
        or "throttl" in text_haystack
    ):
        return TooManyRequestsError(f"429:browser_state:{current_url or title}")
    return None


def wait_for_download(
    driver: webdriver.Firefox,
    download_dir: Path,
    existing_files: Iterable[Path],
    download_timeout_seconds: int,
    poll_interval_seconds: float,
    progress_log_seconds: int,
) -> Path:
    existing_set = set(existing_files)
    deadline = time.monotonic() + download_timeout_seconds
    observed_candidate = None
    observed_size = None
    stable_count = 0
    last_progress = time.monotonic()

    while time.monotonic() < deadline:
        part_files = [path for path in download_dir.glob("*.part") if path.is_file()]
        current_files = list_download_candidates(download_dir)
        new_files = [path for path in current_files if path not in existing_set]

        if progress_log_seconds > 0 and (time.monotonic() - last_progress) >= progress_log_seconds:
            current_sizes = []
            for path in sorted(new_files):
                try:
                    current_sizes.append(f"{path.name}:{path.stat().st_size}")
                except OSError:
                    current_sizes.append(f"{path.name}:?")
            log(
                f"[INFO][pdf][download_wait] part_files={len(part_files)} "
                f"new_files={current_sizes or ['none']}"
            )
            last_progress = time.monotonic()

        if new_files and not part_files:
            candidate = max(new_files, key=lambda path: path.stat().st_mtime)
            size = candidate.stat().st_size
            if candidate == observed_candidate and size == observed_size and size > 0:
                stable_count += 1
            else:
                observed_candidate = candidate
                observed_size = size
                stable_count = 1
            if stable_count >= 2:
                return candidate

        # Si un telechargement est visible (fichier partiel ou nouveau fichier), on
        # privilegie la stabilisation locale avant d'interpreter l'etat navigateur.
        if new_files or part_files:
            time.sleep(poll_interval_seconds)
            continue

        browser_exc = detect_429_from_browser_state(driver)
        if browser_exc is not None:
            raise browser_exc

        time.sleep(poll_interval_seconds)

    raise infer_failure_from_browser_state(driver)


def trigger_download(
    driver: webdriver.Firefox,
    target_url: str,
    click_locator: Optional[Tuple[str, str]],
    click_timeout_seconds: int,
) -> None:
    if click_locator is not None:
        try:
            driver.get(target_url)
        except TimeoutException:
            log(
                f"[WARN][pdf][page_load_timeout] navigation timeout sur {target_url}, "
                "tentative de poursuivre avec le clic"
            )
        log(f"[INFO][pdf][click_wait] locator={click_locator[1]}")
        click_download_trigger(driver, click_locator, click_timeout_seconds)
        log("[INFO][pdf][click_done] attente du telechargement")
        return
    driver.get("about:blank")
    try:
        driver.execute_script("window.location.href = arguments[0];", target_url)
    except TimeoutException:
        # Un telechargement PDF ne correspond pas toujours a un chargement de page classique.
        pass


def file_has_pdf_signature(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return False
        _, prefix = inspect_download_payload(path)
        return is_pdf_signature(prefix)
    except OSError:
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline PDF - Etape 2: telecharge les PDF via Firefox/Selenium."
    )
    parser.add_argument("--input", default="input/arks_numeros.json")
    parser.add_argument("--output", default="input/arks_numeros.json")
    parser.add_argument("--output-csv", default="input/tableau_arks_numeros.csv")
    parser.add_argument("--pdf-root", default="pdf_process")
    parser.add_argument("--requests-per-minute", type=float, default=0.5)
    parser.add_argument("--cb-threshold", type=int, default=5)
    parser.add_argument("--cb-sleep-seconds", type=int, default=600)
    parser.add_argument("--cb-max-cooldowns", type=int, default=1)
    parser.add_argument("--page-timeout-seconds", type=int, default=60)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (Macintosh; Intel Mac OS X 14.0; rv:136.0) Gecko/20100101 Firefox/136.0",
    )
    parser.add_argument("--cookies-file", default="")
    parser.add_argument("--progress-log-seconds", type=int, default=10)
    parser.add_argument("--fail-fast-altcha", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--poll-interval-seconds", type=float, default=0.5)
    parser.add_argument("--click-timeout-seconds", type=int, default=15)
    parser.add_argument("--click-id-prefix", default="customAltcha_checkbox_")
    parser.add_argument("--click-css-selector", default="")
    parser.add_argument("--click-xpath", default="")
    parser.add_argument("--geckodriver-path", default="/opt/homebrew/bin/geckodriver")
    parser.add_argument(
        "--firefox-binary-path",
        default="/Applications/Firefox.app/Contents/MacOS/firefox",
    )
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Affiche Firefox au lieu du mode headless par defaut.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_csv_path = Path(args.output_csv)
    pdf_root = Path(args.pdf_root)
    geckodriver_path = Path(args.geckodriver_path).expanduser().resolve()
    firefox_binary = Path(args.firefox_binary_path).expanduser().resolve()
    download_dir = (pdf_root / "_selenium_downloads").resolve()
    headless = not args.show_browser

    if args.requests_per_minute <= 0:
        raise ValueError("--requests-per-minute doit etre > 0.")
    if args.cb_max_cooldowns < 1:
        raise ValueError("--cb-max-cooldowns doit etre >= 1.")
    if args.page_timeout_seconds < 1:
        raise ValueError("--page-timeout-seconds doit etre >= 1.")
    if args.timeout_seconds < 1:
        raise ValueError("--timeout-seconds doit etre >= 1.")
    if args.poll_interval_seconds <= 0:
        raise ValueError("--poll-interval-seconds doit etre > 0.")
    if args.click_timeout_seconds < 1:
        raise ValueError("--click-timeout-seconds doit etre >= 1.")
    if not geckodriver_path.exists():
        raise FileNotFoundError(f"geckodriver introuvable: {geckodriver_path}")
    if not firefox_binary.exists():
        raise FileNotFoundError(f"Firefox introuvable: {firefox_binary}")

    payload = load_payload(input_path)
    items = ensure_items(payload)
    limiter = RateLimiter(args.requests_per_minute)
    circuit_breaker = CircuitBreaker429(
        args.cb_threshold,
        args.cb_sleep_seconds,
        args.cb_max_cooldowns,
    )
    total_items = len(items)
    with_issue_ark = sum(1 for it in items if str(it.get("issue_ark", "")).strip())
    log(
        f"[INFO][pdf] Demarrage step2 Selenium: total_items={total_items} "
        f"with_issue_ark={with_issue_ark} rpm={args.requests_per_minute:.3f} "
        f"page_timeout={args.page_timeout_seconds}s "
        f"download_timeout={args.timeout_seconds}s force={args.force} headless={headless}"
    )

    download_dir.mkdir(parents=True, exist_ok=True)
    options = build_firefox_options(download_dir, headless, firefox_binary, args.user_agent)
    service = Service(str(geckodriver_path))
    driver = webdriver.Firefox(service=service, options=options)
    driver.set_page_load_timeout(max(args.page_timeout_seconds, 5))
    if args.cookies_file:
        seed_driver_cookies(driver, "https://gallica.bnf.fr/", args.cookies_file)

    click_locator = resolve_click_locator(args)
    downloaded_count = 0
    existing_count = 0
    error_count = 0
    skipped_prior_error = 0
    fatal_stop = False

    try:
        for idx, item in enumerate(items, start=1):
            if str(item.get("status", "")).strip() == "error" and not str(
                item.get("issue_ark", "")
            ).strip():
                log(
                    f"[ERROR][pdf][skip_prior_error][{item.get('revue','')}][{item.get('numero_id','')}] "
                    f"{item.get('error_stage','')} {item.get('error_code','')} {item.get('error_message','')}"
                )
                skipped_prior_error += 1
                continue

            numero_id_raw = str(item.get("numero_id", "")).strip()
            issue_ark_raw = str(item.get("issue_ark", "")).strip()
            numero_id = sanitize_path_part(numero_id_raw, "numero")
            revue_raw = str(item.get("revue", ""))
            log(
                f"[INFO][pdf][progress] item={idx}/{total_items} revue={revue_raw} "
                f"numero={numero_id_raw or 'N/A'}"
            )

            if not numero_id_raw or not issue_ark_raw:
                item["status"] = "error"
                item["pipeline_status"] = "error"
                item["error_stage"] = "pdf_input_validation"
                item["error_code"] = "missing_required_fields"
                item["error_message"] = "numero_id ou issue_ark manquant"
                log(
                    f"[ERROR][pdf][pdf_input_validation][{item.get('revue','')}][{numero_id_raw}] "
                    f"{item['error_message']}"
                )
                error_count += 1
                continue

            try:
                issue_ark = normalize_issue_ark(issue_ark_raw)
            except Exception as exc:
                item["status"] = "error"
                item["pipeline_status"] = "error"
                item["error_stage"] = "pdf_ark_normalization"
                item["error_code"] = error_code_from_exception(exc)
                item["error_message"] = str(exc)
                log(
                    f"[ERROR][pdf][pdf_ark_normalization][{item.get('revue','')}][{numero_id}] "
                    f"{item['error_code']} {item['error_message']}"
                )
                error_count += 1
                continue

            pdf_url = f"https://gallica.bnf.fr/{issue_ark}.pdf"
            revue = sanitize_path_part(str(item.get("revue", "inconnue")), "inconnue")
            output_dir = pdf_root / revue / numero_id
            pdf_path = output_dir / f"{numero_id}.pdf"

            item["pdf_url"] = pdf_url
            item["pdf_path"] = pdf_path.as_posix()
            log(f"[INFO][pdf][url][{item.get('revue','')}][{numero_id}] {pdf_url}")

            if pdf_path.exists() and pdf_path.stat().st_size > 0 and not args.force:
                if file_has_pdf_signature(pdf_path):
                    item["pdf_size_bytes"] = pdf_path.stat().st_size
                    item["status"] = "ok"
                    item["pipeline_status"] = ""
                    item["error_stage"] = ""
                    item["error_code"] = ""
                    item["error_message"] = ""
                    log(
                        f"[INFO][pdf][skip_existing_valid][{item.get('revue','')}][{numero_id}] "
                        f"bytes={item['pdf_size_bytes']}"
                    )
                    existing_count += 1
                    continue
                log(
                    f"[WARN][pdf][existing_invalid_pdf][{item.get('revue','')}][{numero_id}] "
                    f"Fichier existant invalide, re-download: {pdf_path}"
                )
                try:
                    pdf_path.unlink()
                except OSError:
                    pass

            existing_files = list_download_candidates(download_dir)

            try:
                limiter.wait_turn()
                log(
                    f"[INFO][pdf][selenium_start][{item.get('revue','')}][{numero_id}] "
                    f"url={pdf_url} expected={infer_filename_from_url(pdf_url)}"
                )
                trigger_download(
                    driver=driver,
                    target_url=pdf_url,
                    click_locator=click_locator,
                    click_timeout_seconds=args.click_timeout_seconds,
                )
                downloaded_file = wait_for_download(
                    driver=driver,
                    download_dir=download_dir,
                    existing_files=existing_files,
                    download_timeout_seconds=args.timeout_seconds,
                    poll_interval_seconds=args.poll_interval_seconds,
                    progress_log_seconds=args.progress_log_seconds,
                )
                raise_for_non_pdf_payload(downloaded_file)
                output_dir.mkdir(parents=True, exist_ok=True)
                try:
                    if pdf_path.exists():
                        pdf_path.unlink()
                except OSError:
                    pass
                downloaded_file.replace(pdf_path)
                item["pdf_size_bytes"] = pdf_path.stat().st_size
                item["status"] = "ok"
                item["pipeline_status"] = ""
                item["error_stage"] = ""
                item["error_code"] = ""
                item["error_message"] = ""
                circuit_breaker.record_success()
                log(
                    f"[INFO][pdf][downloaded][{item.get('revue','')}][{numero_id}] "
                    f"bytes={item['pdf_size_bytes']}"
                )
                downloaded_count += 1
            except Exception as exc:
                cleanup_new_downloads(download_dir, existing_files)
                cleanup_empty_dirs(output_dir, pdf_root)
                item["status"] = "error"
                item["pipeline_status"] = "error"
                item["error_stage"] = "pdf_download"
                item["error_code"] = error_code_from_exception(exc)
                item["error_message"] = str(exc)
                circuit_breaker.record_failure(exc, context=f"url={pdf_url}")
                log(
                    f"[ERROR][pdf][pdf_download][{item.get('revue','')}][{numero_id}] "
                    f"{item['error_code']} {item['error_message']}"
                )
                error_count += 1
                if args.fail_fast_altcha and item["error_code"] == "captcha_required":
                    fatal_stop = True
                    log(
                        f"[FATAL][pdf][captcha_required] ALTCHA detecte pour {pdf_url}. "
                        "Arret immediat de l'etape 2 (--fail-fast-altcha)."
                    )
                    break

            if idx % 25 == 0:
                log(
                    f"[INFO][pdf][checkpoint] processed={idx}/{total_items} "
                    f"downloaded={downloaded_count} existing={existing_count} "
                    f"errors={error_count} skipped={skipped_prior_error}"
                )
    finally:
        driver.quit()

    payload["total_issues"] = sum(
        1 for it in items if str(it.get("issue_ark", "")).strip()
    )
    payload["total_errors"] = sum(
        1 for it in items if str(it.get("status", "")).strip() == "error"
    )
    payload["total_events"] = len(items)
    payload["pdf_collection"] = {
        "engine": "selenium",
        "requests_per_minute": args.requests_per_minute,
        "pdf_root": pdf_root.as_posix(),
        "download_dir": download_dir.as_posix(),
        "headless": headless,
        "downloaded": downloaded_count,
        "existing": existing_count,
        "errors": error_count,
        "skipped_prior_error": skipped_prior_error,
    }

    save_json(output_path, payload)
    save_csv(output_csv_path, items)
    log(
        f"Termine: {downloaded_count} PDF telecharges, {existing_count} deja presents, "
        f"{error_count} erreurs, {skipped_prior_error} ignores"
    )
    log(f"JSON mis a jour: {output_path}")
    log(f"CSV mis a jour: {output_csv_path}")
    log(f"PDF root: {pdf_root}")
    if fatal_stop:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

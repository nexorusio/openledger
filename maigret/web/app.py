from flask import (
    Flask,
    render_template,
    request,
    send_from_directory,
    Response,
    flash,
    redirect,
    session,
    url_for,
)
from werkzeug.exceptions import NotFound
import logging
import os
import asyncio
import json
import queue
import re
import secrets
import uuid
from datetime import datetime
from threading import Lock, Thread
from typing import Any, Dict
import maigret
import maigret.settings
from maigret.ai import get_ai_analysis_text, validate_openai_connection
from maigret.checking import build_cloudflare_bypass_config
from maigret.result import MaigretCheckStatus
from maigret.sites import MaigretDatabase
from maigret.report import generate_report_context

app = Flask(__name__)
# Use environment variable for secret key, generate random one if not set
app.secret_key = os.getenv('FLASK_SECRET_KEY', os.urandom(24).hex())
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Strict',
    SESSION_COOKIE_SECURE=os.getenv('SESSION_COOKIE_SECURE', 'false').lower()
    in ('true', '1', 'yes'),
)

# add background job tracking
background_jobs: Dict[str, Any] = {}
job_results = {}
analysis_locks: Dict[str, Any] = {}
metadata_lock = Lock()

# Live (streaming) scan jobs, keyed by job_id. Each entry:
#   {'queue': Queue, 'cancelled': bool, 'loop': event loop, 'task': asyncio task}
# Live progress remains in one supervised Gunicorn process and is intentionally
# transient. Terminal results are persisted separately beside their reports.
live_jobs: Dict[str, Any] = {}


class StreamNotify:
    """query_notify shim: pushes each per-site check into a queue as an SSE event.

    maigret's search loop calls update() once per finished site check, which is
    exactly the granularity we want to stream to the browser.
    """

    def __init__(self, event_queue, username):
        self.q = event_queue
        self.username = username
        self.total = 0
        self.checked = 0
        self.sites = {}
        # Per-site results collected so far, in the shape build_reports()
        # expects. If the scan gets cancelled mid-way (Stop button), this is
        # what's left to report on — otherwise every already-streamed
        # 'found' event is silently discarded because the search() task
        # never returns to hand back its own results dict.
        self.results = {}

    def set_total(self, total):
        self.total = total
        self.q.put({'type': 'start', 'username': self.username, 'total': total})

    def set_sites(self, sites):
        self.sites = sites

    def update(self, result, is_similar=False):
        self.checked += 1
        if not is_similar:
            entry = {'status': result, 'url_user': result.site_url_user}
            site = self.sites.get(result.site_name)
            if site is not None:
                entry['site'] = site
                entry['url_main'] = site.url_main
            self.results[result.site_name] = entry
        if result.status == MaigretCheckStatus.CLAIMED and not is_similar:
            ids = {
                k: v
                for k, v in (result.ids_data or {}).items()
                if k != '_extractor' and isinstance(v, (str, int, float))
            }
            self.q.put(
                {
                    'type': 'found',
                    'username': result.username or self.username,
                    'site': result.site_name,
                    'url': result.site_url_user,
                    'ids': ids,
                }
            )
        self.q.put(
            {
                'type': 'progress',
                'checked': self.checked,
                'total': self.total,
                'site': result.site_name,
            }
        )

    # No-op sinks for the rest of the notifier surface the search loop touches.
    def start(self, message=None, id_type="username"):
        pass

    def finish(self, message=None):
        pass

    def warning(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def success(self, *a, **k):
        pass

    def enrich(self, *a, **k):
        pass


# Configuration
app.config["MAIGRET_DB_FILE"] = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), 'resources', 'data.json'
)
app.config["COOKIES_FILE"] = "cookies.txt"
app.config["UPLOAD_FOLDER"] = 'uploads'
app.config["REPORTS_FOLDER"] = os.path.abspath('/tmp/maigret_reports')
app.config["SETTINGS_FILE"] = os.getenv("WEB_SETTINGS_FILE", "web_settings.json")
app.config["OPENAI_API_KEY_FILE"] = os.getenv(
    "OPENAI_API_KEY_FILE",
    os.path.join("runtime", "secrets", "openai_api_key"),
)

# Search-wide defaults, editable from the Settings workspace. Persisted
# to app.config["SETTINGS_FILE"] so they survive a process restart.
DEFAULT_SETTINGS = {
    'timeout': 10,
    'top_sites': 500,
    'tags': [],
    'excluded_tags': [],
    'site_list': [],
    'proxy': '',
    'tor_proxy': '',
    'i2p_proxy': '',
    'permute': False,
    'disable_recursive_search': False,
    'disable_extracting': False,
    'with_domains': False,
    'openai_model': 'gpt-5.4',
}

OPENAI_MODEL_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$')
SESSION_KEY_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$')
SESSION_FOLDER_PATTERN = re.compile(r'^search_[A-Za-z0-9][A-Za-z0-9_-]{0,127}$')
SESSION_METADATA_FILENAME = 'openledger-session.json'
SESSION_METADATA_SCHEMA_VERSION = 1


def load_settings():
    settings = dict(DEFAULT_SETTINGS)
    path = app.config["SETTINGS_FILE"]
    if os.path.exists(path):
        try:
            with open(path, encoding='utf-8') as f:
                settings.update(json.load(f))
        except (json.JSONDecodeError, OSError) as e:
            logging.error(f"Failed to load settings from {path}: {e}")
    return settings


def save_settings(settings):
    with open(app.config["SETTINGS_FILE"], 'w', encoding='utf-8') as f:
        json.dump(settings, f, indent=2)


def get_openai_api_key():
    """Read the API key without exposing it through templates or settings JSON."""
    key_path = app.config.get("OPENAI_API_KEY_FILE")
    if key_path:
        try:
            with open(key_path, encoding='utf-8') as key_file:
                key = key_file.read().strip()
                if key:
                    return key
        except FileNotFoundError:
            pass
        except OSError as exc:
            logging.error("Failed to read the OpenAI key file: %s", exc)
    return os.getenv('OPENAI_API_KEY', '').strip()


def get_openai_key_source():
    key_path = app.config.get("OPENAI_API_KEY_FILE")
    if key_path:
        try:
            with open(key_path, encoding='utf-8') as key_file:
                if key_file.read().strip():
                    return 'protected file'
        except (FileNotFoundError, OSError):
            pass
    if os.getenv('OPENAI_API_KEY', '').strip():
        return 'environment'
    return None


def save_openai_api_key(api_key):
    """Atomically store a key in the protected runtime mount with mode 600."""
    key_path = app.config.get("OPENAI_API_KEY_FILE")
    if not key_path:
        raise RuntimeError('Server-side API key storage is not configured.')

    key_path = os.path.abspath(key_path)
    key_directory = os.path.dirname(key_path)
    os.makedirs(key_directory, mode=0o700, exist_ok=True)
    os.chmod(key_directory, 0o700)
    temporary_path = f"{key_path}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(
        temporary_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        0o600,
    )
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as key_file:
            key_file.write(api_key)
            key_file.write('\n')
            key_file.flush()
            os.fsync(key_file.fileno())
        os.replace(temporary_path, key_path)
        os.chmod(key_path, 0o600)
    except Exception:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise


def remove_openai_api_key():
    key_path = app.config.get("OPENAI_API_KEY_FILE")
    if not key_path:
        return False
    try:
        os.remove(key_path)
        return True
    except FileNotFoundError:
        return False


def is_valid_csrf(provided_token):
    expected_token = session.get('csrf_token')
    return bool(
        expected_token
        and provided_token
        and secrets.compare_digest(expected_token, provided_token)
    )


def parse_settings_form(form):
    current_settings = load_settings()
    try:
        timeout = int(form.get('timeout'))
    except (TypeError, ValueError):
        timeout = DEFAULT_SETTINGS['timeout']

    try:
        top_sites = int(form.get('top_sites'))
    except (TypeError, ValueError):
        top_sites = DEFAULT_SETTINGS['top_sites']

    return {
        'timeout': timeout,
        'top_sites': top_sites,
        'tags': form.getlist('tags'),
        'excluded_tags': form.getlist('excluded_tags'),
        'site_list': [s.strip() for s in form.get('site', '').split(',') if s.strip()],
        'proxy': form.get('proxy', '').strip(),
        'tor_proxy': form.get('tor_proxy', '').strip(),
        'i2p_proxy': form.get('i2p_proxy', '').strip(),
        'permute': 'permute' in form,
        'disable_recursive_search': 'disable_recursive_search' in form,
        'disable_extracting': 'disable_extracting' in form,
        'with_domains': 'with_domains' in form,
        'openai_model': current_settings.get('openai_model', 'gpt-5.4'),
    }


@app.context_processor
def inject_settings():
    return {
        'web_settings': load_settings(),
        'openai_connected': bool(get_openai_api_key()),
        'csrf_token': get_csrf_token(),
    }


def get_available_tags():
    """Load current tags from Maigret's database for the Settings workspace."""
    db = MaigretDatabase().load_from_path(app.config["MAIGRET_DB_FILE"])
    values = {
        tag
        for site in db.sites
        for tag in (getattr(site, 'tags', None) or [])
        if tag
    }
    country_values = {'eu', 'global', 'uk'}
    return [
        {
            'value': tag,
            'label': tag.upper() if len(tag) == 2 else tag.replace('_', ' ').title(),
            'group': (
                'country'
                if len(tag) == 2 or tag in country_values
                else 'category'
            ),
        }
        for tag in sorted(values)
    ]


def setup_logger(log_level, name):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    return logger


async def maigret_search(username, options, query_notify=None):
    logger = setup_logger(logging.WARNING, 'maigret')
    try:
        settings = maigret.settings.Settings()
        settings.load()
        cf_bypass_config = build_cloudflare_bypass_config(settings)
        if cf_bypass_config:
            modules_summary = ", ".join(
                f"{m.get('name', m.get('method'))}({m.get('url')})"
                for m in cf_bypass_config["modules"]
            )
            logger.info(
                f"Cloudflare webgate active: triggers={cf_bypass_config['trigger_protection']}, "
                f"modules=[{modules_summary}]"
            )

        db = MaigretDatabase().load_from_path(app.config["MAIGRET_DB_FILE"])

        top_sites = int(options.get('top_sites') or 500)
        if options.get('all_sites'):
            top_sites = 999999999  # effectively all

        tags = options.get('tags', [])
        excluded_tags = options.get('excluded_tags', [])
        site_list = options.get('site_list', [])
        logger.info(f"Filtering sites by tags: {tags}, excluded: {excluded_tags}")

        sites = db.ranked_sites_dict(
            top=top_sites,
            tags=tags,
            excluded_tags=excluded_tags,
            names=site_list,
            disabled=False,
            id_type='username',
        )

        logger.info(f"Found {len(sites)} sites matching the tag criteria")

        if query_notify is not None and hasattr(query_notify, 'set_total'):
            query_notify.set_total(len(sites))
        if query_notify is not None and hasattr(query_notify, 'set_sites'):
            query_notify.set_sites(sites)

        results = await maigret.search(
            username=username,
            site_dict=sites,
            timeout=int(options.get('timeout', 30)),
            logger=logger,
            id_type='username',
            query_notify=query_notify,
            no_progressbar=bool(query_notify),
            cookies=app.config["COOKIES_FILE"] if options.get('use_cookies') else None,
            is_parsing_enabled=(not options.get('disable_extracting', False)),
            recursive_search_enabled=(
                not options.get('disable_recursive_search', False)
            ),
            check_domains=options.get('with_domains', False),
            proxy=options.get('proxy', None),
            tor_proxy=options.get('tor_proxy', None),
            i2p_proxy=options.get('i2p_proxy', None),
            cloudflare_bypass=cf_bypass_config,
        )
        return results
    except Exception as e:
        logger.error(f"Error during search: {str(e)}")
        raise


async def search_multiple_usernames(usernames, options):
    results = []
    for username in usernames:
        try:
            search_results = await maigret_search(username.strip(), options)
            results.append((username.strip(), 'username', search_results))
        except Exception as e:
            logging.error(f"Error searching username {username}: {str(e)}")
    return results


def sanitize_username_for_path(username: str) -> str:
    """Remove path separators and dangerous components from username for safe file path usage."""
    # Replace path separators and null bytes
    sanitized = username.replace('/', '_').replace('\\', '_').replace('\0', '_')
    # Remove . and .. components
    sanitized = sanitized.strip('.')
    # If empty after sanitization, use a fallback
    return sanitized or '_'


def get_session_metadata_path(session_folder: str) -> str:
    """Return a safe metadata path inside the mounted reports directory."""
    if not isinstance(session_folder, str) or not SESSION_FOLDER_PATTERN.fullmatch(
        session_folder
    ):
        raise ValueError('Invalid report session folder')

    reports_root = os.path.realpath(app.config["REPORTS_FOLDER"])
    session_root = os.path.realpath(os.path.join(reports_root, session_folder))
    if os.path.commonpath([reports_root, session_root]) != reports_root:
        raise ValueError('Invalid report session path')
    return os.path.join(session_root, SESSION_METADATA_FILENAME)


def normalize_persisted_result(session_key: str, result: Dict[str, Any]):
    """Validate and normalize the small JSON-safe result index we persist."""
    if not isinstance(session_key, str) or not SESSION_KEY_PATTERN.fullmatch(
        session_key
    ):
        raise ValueError('Invalid report session key')
    if not isinstance(result, dict):
        raise ValueError('Invalid report session metadata')

    status = result.get('status')
    if status not in {'completed', 'failed'}:
        raise ValueError('Only terminal investigation results can be persisted')

    expected_folder = f'search_{session_key}'
    session_folder = result.get('session_folder') or expected_folder
    if session_folder != expected_folder:
        raise ValueError('Report session folder does not match its key')

    usernames = result.get('usernames', [])
    if not isinstance(usernames, list) or not all(
        isinstance(username, str) for username in usernames
    ):
        raise ValueError('Invalid usernames in report session metadata')

    normalized = dict(result)
    normalized['session_folder'] = expected_folder
    normalized['usernames'] = usernames
    if status == 'completed':
        if not isinstance(normalized.get('graph_file'), str) or not isinstance(
            normalized.get('individual_reports'), list
        ):
            raise ValueError('Incomplete report session metadata')
        found_count = normalized.get('found_count', 0)
        if not isinstance(found_count, int) or found_count < 0:
            raise ValueError('Invalid profile count in report session metadata')
        normalized['found_count'] = found_count
    else:
        normalized['error'] = str(normalized.get('error', 'Unknown error occurred.'))
    return normalized


def persist_job_result(session_key: str, result: Dict[str, Any]):
    """Atomically persist terminal job metadata alongside its report files."""
    normalized = normalize_persisted_result(session_key, result)
    metadata_path = get_session_metadata_path(normalized['session_folder'])
    metadata_directory = os.path.dirname(metadata_path)
    os.makedirs(metadata_directory, mode=0o700, exist_ok=True)

    payload = {
        'schema_version': SESSION_METADATA_SCHEMA_VERSION,
        'session_key': session_key,
        'result': normalized,
    }
    temporary_path = f"{metadata_path}.{uuid.uuid4().hex}.tmp"

    with metadata_lock:
        descriptor = os.open(
            temporary_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            with os.fdopen(descriptor, 'w', encoding='utf-8') as metadata_file:
                json.dump(payload, metadata_file, indent=2)
                metadata_file.write('\n')
                metadata_file.flush()
                os.fsync(metadata_file.fileno())
            os.replace(temporary_path, metadata_path)
            os.chmod(metadata_path, 0o600)
        except Exception:
            try:
                os.unlink(temporary_path)
            except FileNotFoundError:
                pass
            raise
    return normalized


def record_job_result(session_key: str, result: Dict[str, Any]):
    """Publish a terminal result in memory and durably when storage is available."""
    normalized = normalize_persisted_result(session_key, result)
    job_results[session_key] = normalized
    try:
        persist_job_result(session_key, normalized)
    except (OSError, TypeError, ValueError):
        logging.exception('Failed to persist investigation metadata for %s', session_key)
    return normalized


def load_persisted_job_result(session_folder: str):
    """Load and validate one persisted result without trusting its file contents."""
    try:
        metadata_path = get_session_metadata_path(session_folder)
        with open(metadata_path, encoding='utf-8') as metadata_file:
            payload = json.load(metadata_file)
        if payload.get('schema_version') != SESSION_METADATA_SCHEMA_VERSION:
            raise ValueError('Unsupported report session metadata version')
        session_key = payload.get('session_key')
        result = normalize_persisted_result(session_key, payload.get('result'))
        if result['session_folder'] != session_folder:
            raise ValueError('Report session metadata is in the wrong directory')
        return session_key, result
    except FileNotFoundError:
        return None
    except (AttributeError, json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
        logging.warning(
            'Ignoring invalid investigation metadata in %s: %s', session_folder, exc
        )
        return None


def refresh_job_results_from_disk():
    """Rebuild terminal job state from the persistent reports mount."""
    reports_root = app.config["REPORTS_FOLDER"]
    try:
        entries = list(os.scandir(reports_root))
    except FileNotFoundError:
        return 0
    except OSError as exc:
        logging.warning('Could not read persisted investigation history: %s', exc)
        return 0

    loaded_count = 0
    for entry in entries:
        if not entry.is_dir() or not SESSION_FOLDER_PATTERN.fullmatch(entry.name):
            continue
        loaded = load_persisted_job_result(entry.name)
        if not loaded:
            continue
        session_key, result = loaded
        if session_key not in job_results:
            job_results[session_key] = result
            loaded_count += 1
    return loaded_count


def find_result_by_session(session_id: str):
    """Resolve a session from memory, then recover it from persistent storage."""
    result = next(
        (
            result
            for result in job_results.values()
            if result.get('status') == 'completed'
            and result.get('session_folder') == session_id
        ),
        None,
    )
    if result:
        return result

    loaded = load_persisted_job_result(session_id)
    if not loaded:
        return None
    session_key, result = loaded
    job_results[session_key] = result
    return result if result.get('status') == 'completed' else None


# Rebuild the terminal result index when Flask is imported by Gunicorn. Routes
# also perform targeted lazy recovery so alternate report paths used in tests or
# embedded deployments remain supported.
refresh_job_results_from_disk()


def build_ai_markdown(result_data: Dict[str, Any]) -> str:
    """Create a bounded, normalized AI input from completed claimed profiles."""
    lines = [
        '# OpenLedger username investigation',
        '',
        'Treat matches as investigative leads, not verified identity evidence.',
        '',
    ]
    for report in result_data.get('individual_reports', []):
        lines.extend([f"## Username: {report.get('username', 'unknown')}", ''])
        profiles = report.get('claimed_profiles', [])
        if not profiles:
            lines.extend(['No claimed profiles were found.', ''])
            continue
        for profile in profiles:
            tags = ', '.join(profile.get('tags') or []) or 'none'
            lines.append(
                f"- {profile.get('site_name', 'Unknown site')}: "
                f"{profile.get('url', '')} (tags: {tags})"
            )
        lines.append('')

    # Bound cost and prevent an unexpectedly large model request.
    return '\n'.join(lines)[:100_000]


def get_csrf_token() -> str:
    token = session.get('csrf_token')
    if not token:
        token = secrets.token_urlsafe(32)
        session['csrf_token'] = token
    return token


def get_analysis_path(result_data: Dict[str, Any]) -> str:
    reports_root = os.path.realpath(app.config["REPORTS_FOLDER"])
    session_folder = result_data['session_folder']
    session_root = os.path.realpath(os.path.join(reports_root, session_folder))
    if os.path.commonpath([reports_root, session_root]) != reports_root:
        raise ValueError('Invalid report session path')
    return os.path.join(session_root, 'ai_analysis.md')


def build_reports(general_results, usernames, session_key):
    """Write per-username CSV/JSON/PDF/HTML reports + combined graph to disk.

    Shared by the background /search job and the live SSE /api/scan job, so
    both flows land on the same results.html (report buttons + profile list).
    """
    os.makedirs(app.config["REPORTS_FOLDER"], exist_ok=True)
    session_folder = os.path.join(
        app.config["REPORTS_FOLDER"], f"search_{session_key}"
    )
    os.makedirs(session_folder, exist_ok=True)

    graph_path = os.path.join(session_folder, "combined_graph.html")
    maigret.report.save_graph_report(
        graph_path,
        general_results,
        MaigretDatabase().load_from_path(app.config["MAIGRET_DB_FILE"]),
    )

    individual_reports = []
    found_count = 0
    for username, id_type, results in general_results:
        safe_username = sanitize_username_for_path(username)
        report_base = os.path.join(session_folder, f"report_{safe_username}")

        csv_path = f"{report_base}.csv"
        json_path = f"{report_base}.json"
        pdf_path = f"{report_base}.pdf"
        html_path = f"{report_base}.html"

        context = generate_report_context(general_results)

        maigret.report.save_csv_report(csv_path, username, results)
        maigret.report.save_json_report(
            json_path, username, results, report_type='ndjson'
        )
        maigret.report.save_pdf_report(pdf_path, context)
        maigret.report.save_html_report(html_path, context)

        claimed_profiles = []
        for site_name, site_data in results.items():
            if (
                site_data.get('status')
                and site_data['status'].status == MaigretCheckStatus.CLAIMED
            ):
                claimed_profiles.append(
                    {
                        'site_name': site_name,
                        'url': site_data.get('url_user', ''),
                        'tags': (
                            site_data.get('status').tags
                            if site_data.get('status')
                            else []
                        ),
                    }
                )

        found_count += len(claimed_profiles)
        individual_reports.append(
            {
                'username': username,
                'csv_file': os.path.join(
                    f"search_{session_key}", f"report_{safe_username}.csv"
                ),
                'json_file': os.path.join(
                    f"search_{session_key}", f"report_{safe_username}.json"
                ),
                'pdf_file': os.path.join(
                    f"search_{session_key}", f"report_{safe_username}.pdf"
                ),
                'html_file': os.path.join(
                    f"search_{session_key}", f"report_{safe_username}.html"
                ),
                'claimed_profiles': claimed_profiles,
            }
        )

    return {
        'status': 'completed',
        'session_folder': f"search_{session_key}",
        'graph_file': os.path.join(f"search_{session_key}", "combined_graph.html"),
        'usernames': usernames,
        'individual_reports': individual_reports,
        'found_count': found_count,
    }


def process_search_task(usernames, options, timestamp):
    started_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    result = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        general_results = loop.run_until_complete(
            search_multiple_usernames(usernames, options)
        )
        result = build_reports(general_results, usernames, timestamp)

    except Exception as e:
        logging.error(f"Error in search task for timestamp {timestamp}: {str(e)}")
        result = {
            'status': 'failed',
            'error': str(e),
            'usernames': usernames,
        }
    finally:
        if result is None:
            result = {
                'status': 'failed',
                'error': 'The investigation ended without a result.',
                'usernames': usernames,
            }
        result['started_at'] = started_at
        record_job_result(timestamp, result)
        if timestamp in background_jobs:
            background_jobs[timestamp]['completed'] = True


def parse_usernames(form):
    usernames_input = form.get('usernames', '').strip()
    return [u.strip() for u in usernames_input.replace(',', ' ').split() if u.strip()]


def parse_search_options(form):
    settings = load_settings()
    return {
        'top_sites': settings['top_sites'],
        'timeout': settings['timeout'],
        'use_cookies': 'use_cookies' in form,
        'all_sites': form.get('mode') == 'full',
        'disable_recursive_search': settings['disable_recursive_search'],
        'disable_extracting': settings['disable_extracting'],
        'with_domains': settings['with_domains'],
        'proxy': settings['proxy'] or None,
        'tor_proxy': settings['tor_proxy'] or None,
        'i2p_proxy': settings['i2p_proxy'] or None,
        'permute': settings['permute'],
        'tags': settings['tags'],
        'excluded_tags': settings['excluded_tags'],
        'site_list': settings['site_list'],
    }


async def _stream_search(job, usernames, options):
    q = job['queue']
    general_results = []
    for username in usernames:
        if job['cancelled']:
            break
        notify = StreamNotify(q, username.strip())
        task = asyncio.ensure_future(
            maigret_search(username.strip(), options, query_notify=notify)
        )
        job['task'] = task
        try:
            results = await task
            general_results.append((username.strip(), 'username', results))
        except asyncio.CancelledError:
            # The task never got to return its own results dict, but every
            # site checked before cancellation already streamed a 'found' /
            # 'progress' event and was captured by the notifier — report on
            # that instead of throwing it away.
            if notify.results:
                general_results.append((username.strip(), 'username', notify.results))
            q.put({'type': 'stopped', 'username': username.strip()})
            break
        except Exception as e:
            if notify.results:
                general_results.append((username.strip(), 'username', notify.results))
            q.put({'type': 'error', 'message': str(e), 'username': username.strip()})
    return general_results


def run_stream_job(job_id, usernames, options):
    started_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    job = live_jobs[job_id]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    job['loop'] = loop
    general_results = []
    try:
        general_results = loop.run_until_complete(
            _stream_search(job, usernames, options)
        )
    except Exception as e:
        job['queue'].put({'type': 'error', 'message': str(e)})
    finally:
        loop.close()

    # Same report files + results page as the classic /search flow, so the
    # live graph is a progress view, not a replacement for the report.
    done_event = {'type': 'done'}
    if general_results:
        try:
            result = build_reports(general_results, usernames, job_id)
            result['started_at'] = started_at
            record_job_result(job_id, result)
            done_event['redirect'] = f"/results/search_{job_id}"
        except Exception as e:
            logging.error(f"Error building reports for live scan {job_id}: {str(e)}")
            record_job_result(
                job_id,
                {
                    'status': 'failed',
                    'error': str(e),
                    'usernames': usernames,
                    'started_at': started_at,
                },
            )
    else:
        record_job_result(
            job_id,
            {
                'status': 'failed',
                'error': 'The investigation produced no reportable results.',
                'usernames': usernames,
                'started_at': started_at,
            },
        )
    job['queue'].put(done_event)


def start_live_job(usernames, options):
    job_id = uuid.uuid4().hex
    live_jobs[job_id] = {
        'queue': queue.Queue(),
        'cancelled': False,
        'loop': None,
        'task': None,
    }
    Thread(target=run_stream_job, args=(job_id, usernames, options)).start()
    return job_id


@app.route('/api/scan', methods=['POST'])
def scan_start():
    usernames = parse_usernames(request.form)
    if not usernames:
        return {'error': 'At least one username is required'}, 400

    options = parse_search_options(request.form)
    job_id = start_live_job(usernames, options)
    return {'job_id': job_id}


@app.route('/api/scan/<job_id>/stream')
def scan_stream(job_id):
    job = live_jobs.get(job_id)
    if not job:
        return "Unknown job", 404

    def gen():
        try:
            while True:
                event = job['queue'].get()
                yield f"data: {json.dumps(event)}\n\n"
                if event.get('type') == 'done':
                    break
        finally:
            live_jobs.pop(job_id, None)

    return Response(gen(), mimetype='text/event-stream')


@app.route('/api/scan/<job_id>/stop', methods=['POST'])
def scan_stop(job_id):
    job = live_jobs.get(job_id)
    if not job:
        return {'error': 'unknown job'}, 404

    job['cancelled'] = True
    loop = job.get('loop')
    task = job.get('task')
    if loop and task:
        loop.call_soon_threadsafe(task.cancel)
    return {'ok': True}


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/healthz')
def healthz():
    return {'status': 'ok'}


@app.route('/api/sites')
def api_sites():
    """Site names/URLs for the Filters site-picker datalist, fetched lazily
    from Settings instead of loading the DB on every page render."""
    db = MaigretDatabase().load_from_path(app.config["MAIGRET_DB_FILE"])
    site_options = []
    for site in db.sites:
        site_options.append(site.name)
        if site.url_main and site.url_main not in site_options:
            site_options.append(site.url_main)
    return {'sites': sorted(set(site_options))}


@app.route('/settings', methods=['GET', 'POST'])
def settings_update():
    if request.method == 'GET':
        return render_template(
            'settings.html',
            available_tags=get_available_tags(),
            openai_key_source=get_openai_key_source(),
        )

    if not is_valid_csrf(request.form.get('csrf_token')):
        flash('Your settings session expired. Please try again.', 'danger')
        return redirect(url_for('settings_update'))

    save_settings(parse_settings_form(request.form))
    flash('Settings saved.', 'success')
    return redirect(url_for('settings_update'))


@app.route('/settings/openai', methods=['POST'])
def openai_settings_update():
    if not is_valid_csrf(request.form.get('csrf_token')):
        flash('Your settings session expired. Please try again.', 'danger')
        return redirect(url_for('settings_update', section='connections'))

    action = request.form.get('action', 'connect')
    if action == 'disconnect':
        if get_openai_key_source() == 'environment':
            flash(
                'This key is managed by the server environment and cannot be '
                'removed from the browser.',
                'warning',
            )
        elif remove_openai_api_key():
            flash('OpenAI connection removed.', 'success')
        else:
            flash('No browser-managed OpenAI connection was configured.', 'info')
        return redirect(url_for('settings_update', section='connections'))

    model = request.form.get('openai_model', '').strip()
    if not OPENAI_MODEL_PATTERN.fullmatch(model):
        flash('Enter a valid OpenAI model ID.', 'danger')
        return redirect(url_for('settings_update', section='connections'))

    submitted_key = request.form.get('openai_api_key', '').strip()
    candidate_key = submitted_key or get_openai_api_key()
    if not candidate_key:
        flash('Enter an OpenAI API key to connect.', 'danger')
        return redirect(url_for('settings_update', section='connections'))

    try:
        confirmed_model = asyncio.run(
            validate_openai_connection(
                api_key=candidate_key,
                model=model,
                api_base_url=os.getenv(
                    'OPENAI_API_BASE_URL', 'https://api.openai.com/v1'
                ),
            )
        )
    except Exception:
        logging.exception('OpenAI connection verification failed')
        flash(
            'OpenAI verification failed. Check the API key, model access, and '
            'server logs.',
            'danger',
        )
        return redirect(url_for('settings_update', section='connections'))

    if submitted_key:
        save_openai_api_key(submitted_key)
    settings = load_settings()
    settings['openai_model'] = confirmed_model
    save_settings(settings)
    flash('OpenAI connected and verified.', 'success')
    return redirect(url_for('settings_update', section='connections'))


@app.route('/history')
def history():
    refresh_job_results_from_disk()
    entries = sorted(
        job_results.values(), key=lambda r: r.get('started_at', ''), reverse=True
    )
    return render_template('history.html', entries=entries)


@app.route('/live', methods=['POST'])
def live_start():
    usernames = parse_usernames(request.form)
    if not usernames:
        flash('At least one username is required', 'danger')
        return redirect(url_for('index'))

    options = parse_search_options(request.form)
    job_id = start_live_job(usernames, options)
    return redirect(url_for('live_results', job_id=job_id))


@app.route('/live/<job_id>')
def live_results(job_id):
    result = job_results.get(job_id)
    if not result:
        loaded = load_persisted_job_result(f'search_{job_id}')
        if loaded:
            _, result = loaded
            job_results[job_id] = result
    if job_id not in live_jobs and not result:
        flash('Unknown or expired scan session.', 'danger')
        return redirect(url_for('index'))

    done_redirect = None
    if result and result.get('status') == 'completed':
        done_redirect = url_for('results', session_id=result['session_folder'])

    return render_template('live.html', job_id=job_id, done_redirect=done_redirect)


# Modified search route
@app.route('/search', methods=['POST'])
def search():
    usernames = parse_usernames(request.form)
    if not usernames:
        flash('At least one username is required', 'danger')
        return redirect(url_for('index'))

    # Create timestamp for this search session
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    options = parse_search_options(request.form)
    logging.info(
        f"Starting search for usernames: {usernames} with tags: {options['tags']}, "
        f"excluded: {options['excluded_tags']}"
    )

    # Start background job
    background_jobs[timestamp] = {
        'completed': False,
        'thread': Thread(
            target=process_search_task, args=(usernames, options, timestamp)
        ),
    }
    background_jobs[timestamp]['thread'].start()  # type: ignore[union-attr]

    return redirect(url_for('status', timestamp=timestamp))


@app.route('/status/<timestamp>')
def status(timestamp):
    logging.info(f"Status check for timestamp: {timestamp}")

    # A completed job can be reopened after a process or container restart even
    # though its transient background thread entry no longer exists.
    if timestamp not in background_jobs:
        result = job_results.get(timestamp)
        if not result:
            loaded = load_persisted_job_result(f'search_{timestamp}')
            if loaded:
                _, result = loaded
                job_results[timestamp] = result
        if result and result.get('status') == 'completed':
            return redirect(url_for('results', session_id=result['session_folder']))
        if result and result.get('status') == 'failed':
            error_msg = result.get('error', 'Unknown error occurred.')
            flash(f'Search failed: {error_msg}', 'danger')
            return redirect(url_for('history'))
        flash('Invalid search session.', 'danger')
        logging.error(f"Invalid search session: {timestamp}")
        return redirect(url_for('index'))

    # Check if job is completed
    if background_jobs[timestamp]['completed']:
        result = job_results.get(timestamp)
        if not result:
            flash('No results found for this search session.', 'warning')
            logging.error(f"No results found for completed session: {timestamp}")
            return redirect(url_for('index'))

        if result['status'] == 'completed':
            # Note: use the session_folder from the results to redirect
            return redirect(url_for('results', session_id=result['session_folder']))
        else:
            error_msg = result.get('error', 'Unknown error occurred.')
            flash(f'Search failed: {error_msg}', 'danger')
            logging.error(f"Search failed for session {timestamp}: {error_msg}")
            return redirect(url_for('index'))

    # If job is still running, show a status page
    return render_template('status.html', timestamp=timestamp)


@app.route('/results/<session_id>')
def results(session_id):
    result_data = find_result_by_session(session_id)

    if not result_data:
        flash('No results found for this session ID.', 'danger')
        logging.error(f"Results for session {session_id} not found in job_results.")
        return redirect(url_for('index'))

    return render_template(
        'results.html',
        usernames=result_data['usernames'],
        graph_file=result_data['graph_file'],
        individual_reports=result_data['individual_reports'],
        found_count=result_data.get('found_count', 0),
        timestamp=session_id.replace('search_', ''),
        session_id=session_id,
        ai_enabled=bool(get_openai_api_key()),
        csrf_token=get_csrf_token(),
    )


@app.route('/api/analysis/<session_id>', methods=['POST'])
def analyze_session(session_id):
    provided_token = request.headers.get('X-OpenLedger-CSRF', '')
    if not is_valid_csrf(provided_token):
        return {'error': 'Invalid request token. Refresh the results page.'}, 403

    api_key = get_openai_api_key()
    if not api_key:
        return {'error': 'AI analysis is not configured on the server.'}, 503

    result_data = find_result_by_session(session_id)
    if not result_data:
        return {'error': 'Unknown or expired scan session.'}, 404

    lock = analysis_locks.setdefault(session_id, Lock())
    if not lock.acquire(blocking=False):
        return {'error': 'Analysis is already running for this session.'}, 409

    try:
        analysis_path = get_analysis_path(result_data)
        if os.path.exists(analysis_path):
            with open(analysis_path, encoding='utf-8') as analysis_file:
                return {'analysis': analysis_file.read(), 'cached': True}

        markdown_report = build_ai_markdown(result_data)
        model = load_settings().get(
            'openai_model', os.getenv('OPENAI_MODEL', 'gpt-5.4')
        )
        api_base_url = os.getenv(
            'OPENAI_API_BASE_URL', 'https://api.openai.com/v1'
        )
        analysis = asyncio.run(
            get_ai_analysis_text(
                api_key=api_key,
                markdown_report=markdown_report,
                model=model,
                api_base_url=api_base_url,
            )
        )

        os.makedirs(os.path.dirname(analysis_path), exist_ok=True)
        temporary_path = f"{analysis_path}.{uuid.uuid4().hex}.tmp"
        with open(temporary_path, 'w', encoding='utf-8') as analysis_file:
            analysis_file.write(analysis)
            analysis_file.write('\n')
        os.replace(temporary_path, analysis_path)
        return {'analysis': analysis, 'cached': False}
    except Exception:
        logging.exception('AI analysis failed for session %s', session_id)
        return {
            'error': 'AI analysis failed. Check the OpenLedger server logs.'
        }, 502
    finally:
        lock.release()


@app.route('/reports/<path:filename>')
def download_report(filename):
    reports_root = app.config["REPORTS_FOLDER"]
    os.makedirs(reports_root, exist_ok=True)
    if os.path.basename(filename) == SESSION_METADATA_FILENAME:
        return "File not found", 404
    try:
        return send_from_directory(reports_root, filename)
    except NotFound:
        return "File not found", 404
    except Exception as e:
        logging.error(f"Error serving file {filename}: {str(e)}")
        return "File not found", 404


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() in ['true', '1', 't']

    # Host configuration: secure by default
    # Use 127.0.0.1 for local development, 0.0.0.0 only if explicitly set
    host = os.getenv('FLASK_HOST', '127.0.0.1')
    port = int(os.getenv('FLASK_PORT', '5000'))

    app.run(host=host, port=port, debug=debug_mode)

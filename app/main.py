import json
import logging
import os
import re
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse, urlunparse

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, field_validator
from zoneinfo import ZoneInfo


TZ_NAME = os.environ.get("TZ", "Asia/Shanghai")
TZ = ZoneInfo(TZ_NAME)
CONFIG_PATH = Path(os.environ.get("ANISTRM_CONFIG_PATH", "/data/config.json"))
OUTPUT_DIR = Path(os.environ.get("ANISTRM_OUTPUT_DIR", "/strm"))

DEFAULT_CONFIG = {
    "enabled": True,
    "full_sync_once": False,
    "use_proxy": False,
    "http_proxy": "",
    "proxy_base": "https://openani.an-i.workers.dev",
    "selected_seasons": ["latest"],
    "cron": "20 22,23,0,1 * * *",
    "output_dir": str(OUTPUT_DIR),
}


class MemoryLogHandler(logging.Handler):
    def __init__(self, capacity: int = 300):
        super().__init__()
        self.records = deque(maxlen=capacity)
        self.records_lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        line = self.format(record)
        with self.records_lock:
            self.records.append(line)

    def tail(self, limit: int = 120) -> List[str]:
        with self.records_lock:
            return list(self.records)[-limit:]


logger = logging.getLogger("anistrm")
logger.setLevel(logging.INFO)
logger.propagate = False
stream_handler = logging.StreamHandler()
memory_handler = MemoryLogHandler()
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
stream_handler.setFormatter(formatter)
memory_handler.setFormatter(formatter)
logger.handlers = [stream_handler, memory_handler]


class ConfigModel(BaseModel):
    enabled: bool = True
    full_sync_once: bool = False
    use_proxy: bool = False
    http_proxy: str = ""
    proxy_base: str = "https://openani.an-i.workers.dev"
    selected_seasons: List[str] = Field(default_factory=lambda: ["latest"])
    cron: str = "20 22,23,0,1 * * *"
    output_dir: str = str(OUTPUT_DIR)

    @field_validator("proxy_base")
    @classmethod
    def normalize_proxy_base(cls, value: str) -> str:
        value = (value or DEFAULT_CONFIG["proxy_base"]).strip().rstrip("/")
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("反代地址必须是 http 或 https URL")
        return value

    @field_validator("selected_seasons")
    @classmethod
    def normalize_seasons(cls, value: List[str]) -> List[str]:
        seasons = []
        for item in value or []:
            item = str(item).strip()
            if item and item not in seasons:
                seasons.append(item)
        return seasons

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, value: str) -> str:
        value = (value or "").strip()
        if not value:
            raise ValueError("cron 不能为空")
        try:
            CronTrigger.from_crontab(value)
        except Exception as exc:
            raise ValueError(f"cron 格式无效: {exc}") from exc
        return value

    @field_validator("output_dir")
    @classmethod
    def normalize_output_dir(cls, value: str) -> str:
        value = (value or str(OUTPUT_DIR)).strip()
        if Path(value) != OUTPUT_DIR:
            raise ValueError(f"输出目录固定为 {OUTPUT_DIR}")
        return value


class RunResult(BaseModel):
    seasons: List[str]
    total_files: int
    created: int
    exists: int
    failed: int
    started_at: str
    finished_at: str


class StateModel(BaseModel):
    running: bool = False
    last_run_started_at: Optional[str] = None
    last_run_finished_at: Optional[str] = None
    last_result: Optional[RunResult] = None
    last_error: Optional[str] = None
    next_run_at: Optional[str] = None


class AniStrmClient:
    FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
    SPECIAL_FOLDERS = ("ANi",)

    def __init__(
        self,
        proxy_base: str,
        use_proxy: bool = False,
        http_proxy: str = "",
        timeout: int = 30,
        session_factory: Callable[[], requests.Session] = requests.Session,
    ):
        self.proxy_base = proxy_base.rstrip("/")
        self.use_proxy = use_proxy
        self.http_proxy = http_proxy.strip()
        self.timeout = timeout
        self.session_factory = session_factory

    def get_current_season(self, idx_month: Optional[int] = None, now: Optional[datetime] = None) -> str:
        remote_season = self._get_latest_remote_season()
        if remote_season:
            return remote_season
        return self._get_local_season(idx_month=idx_month, now=now)

    def _get_local_season(self, idx_month: Optional[int] = None, now: Optional[datetime] = None) -> str:
        current = now or datetime.now(TZ)
        current_month = idx_month or current.month
        season_month = ((current_month - 1) // 3) * 3 + 1
        return f"{current.year}-{season_month}"

    def get_available_seasons(self) -> List[str]:
        payload = self._fetch_folder_payload(f"{self.proxy_base}/")
        seasons = []
        for file_info in payload.get("files") or []:
            name = file_info.get("name") or ""
            mime_type = file_info.get("mimeType") or ""
            if mime_type != self.FOLDER_MIME_TYPE:
                continue
            if self._parse_season(name) or name in self.SPECIAL_FOLDERS:
                seasons.append(name)
        seasons.sort(key=self._parse_season_sort_key, reverse=True)
        return seasons

    def get_season_entries(self, season: str) -> List[Dict[str, str]]:
        return self._with_retry(lambda: self._collect_folder_entries(f"{season}/", season=season), default=[])

    def _get_latest_remote_season(self) -> Optional[str]:
        return self._with_retry(
            lambda: self._extract_latest_season(self._fetch_folder_payload(f"{self.proxy_base}/").get("files") or []),
            default=None,
        )

    def _fetch_folder_payload(self, url: str) -> Dict[str, Any]:
        session = self.session_factory()
        try:
            proxies = None
            if self.use_proxy and self.http_proxy:
                proxies = {"http": self.http_proxy, "https": self.http_proxy}
            response = session.post(url=url, data='{"password":""}', timeout=self.timeout, proxies=proxies)
            response.raise_for_status()
            return response.json()
        finally:
            session.close()

    def _collect_folder_entries(
        self,
        folder_path: str,
        relative_dir: str = "",
        season: str = "",
    ) -> List[Dict[str, str]]:
        payload = self._fetch_folder_payload(f"{self.proxy_base}/{folder_path}")
        entries: List[Dict[str, str]] = []
        for file_info in payload.get("files") or []:
            name = file_info.get("name") or ""
            if not name:
                continue
            mime_type = file_info.get("mimeType") or ""
            if mime_type == self.FOLDER_MIME_TYPE:
                child_relative_dir = f"{relative_dir}/{name}".strip("/")
                child_folder_path = f"{folder_path.rstrip('/')}/{quote(name, safe='')}/"
                try:
                    entries.extend(self._collect_folder_entries(child_folder_path, child_relative_dir, season=season))
                except Exception as exc:
                    logger.warning("跳过读取失败的目录：%s - %s", child_relative_dir or name, exc)
                continue

            encoded_name = quote(name, safe="")
            file_url = f"{self.proxy_base}/{folder_path.rstrip('/')}/{encoded_name}"
            entries.append({"name": name, "url": file_url, "relative_dir": relative_dir, "season": season})
        return entries

    @staticmethod
    def normalize_stream_url(url: str) -> str:
        if url.endswith(".mp4"):
            return url
        if url.endswith(".mp4?d=true"):
            return url[:-7]
        if "?d=mp4" in url:
            return url.replace("?d=mp4", ".mp4")
        if "?d=true" in url and ".mp4?d=true" not in url:
            return url.replace("?d=true", "")
        return f"{url}.mp4"

    @staticmethod
    def normalize_stream_link(link: str, proxy_base: str) -> str:
        parsed = urlparse(link)
        if parsed.netloc not in {"resources.ani.rip", "openani.an-i.workers.dev"}:
            return link
        proxy_parsed = urlparse(proxy_base)
        return urlunparse(
            (proxy_parsed.scheme, proxy_parsed.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
        )

    @staticmethod
    def _with_retry(operation: Callable[[], Any], default: Any, tries: int = 3, delay: int = 3) -> Any:
        wait_seconds = delay
        for attempt in range(1, tries + 1):
            try:
                return operation()
            except Exception as exc:
                if attempt == tries:
                    logger.warning("ANiStrm 请求失败，已达到最大重试次数：%s", exc)
                    break
                logger.warning("未获取到文件信息，%s 秒后重试 ...", wait_seconds)
                time.sleep(wait_seconds)
        logger.warning("请确保季度番剧文件夹存在或检查网络问题")
        return default

    @staticmethod
    def _parse_season(name: str) -> Optional[Tuple[int, int]]:
        match = re.fullmatch(r"(\d{4})-(\d{1,2})", name)
        if not match:
            return None
        return int(match.group(1)), int(match.group(2))

    @classmethod
    def _parse_season_sort_key(cls, name: str) -> Tuple[int, int]:
        return cls._parse_season(name) or (0, 0)

    @classmethod
    def _extract_latest_season(cls, files: List[Dict[str, str]]) -> Optional[str]:
        seasons: List[Tuple[int, int]] = []
        for file_info in files:
            name = file_info.get("name") or ""
            mime_type = file_info.get("mimeType") or ""
            if mime_type == cls.FOLDER_MIME_TYPE and cls._parse_season(name):
                seasons.append(cls._parse_season(name))
        if not seasons:
            return None
        year, month = max(seasons)
        return f"{year}-{month}"


class StrmFileService:
    INVALID_CHARS = re.compile(r'[<>:"\\|?*\x00-\x1f]')
    VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".webm"}
    DIRECT_DOWNLOAD_EXTENSIONS = {
        ".nfo",
        ".srt",
        ".vtt",
        ".ass",
        ".ssa",
        ".smi",
        ".jpg",
        ".jpeg",
        ".png",
        ".webp",
        ".zip",
    }
    EPISODE_LABELS = (
        "OVA",
        "OAD",
        "SP",
        "PV",
        "CM",
        "NCOP",
        "NCED",
        "\u96fb\u5f71",
        "\u5287\u5834\u7248",
        "\u5267\u573a\u7247",
    )
    @classmethod
    def safe_path_part(cls, value: str) -> str:
        value = cls.INVALID_CHARS.sub("_", value).strip().rstrip(".")
        return value or "_"

    @staticmethod
    def file_extension(file_name: str) -> str:
        return Path(file_name).suffix.lower()

    @classmethod
    def infer_series_dir(cls, file_name: str) -> str:
        stem = file_name
        for suffix in (
            ".strm",
            ".mp4",
            ".mkv",
            ".avi",
            ".mov",
            ".flv",
            ".webm",
            ".nfo",
            ".srt",
            ".ass",
            ".ssa",
            ".smi",
            ".vtt",
            ".jpg",
            ".jpeg",
            ".png",
            ".webp",
            ".zip",
        ):
            if stem.lower().endswith(suffix):
                stem = stem[: -len(suffix)]
                break
        stem = re.sub(r"^\[[^\]]+\]\s*", "", stem).strip()
        for delimiter in re.finditer(r"\s-\s", stem):
            tail = stem[delimiter.end() :].strip()
            if not tail:
                continue
            first_token = re.split(r"[\s\[]", tail, maxsplit=1)[0]
            if first_token.isdigit() or any(tail.startswith(label) for label in cls.EPISODE_LABELS):
                series = stem[: delimiter.start()].strip()
                if series:
                    return series
        return stem.strip() or file_name

    def build_directory(self, storage_path: Path, file_name: str, relative_dir: Optional[str] = None) -> Path:
        directory = storage_path
        if relative_dir:
            series_dir = Path(relative_dir).parts[0]
            return directory / self.safe_path_part(series_dir)
        return directory / self.safe_path_part(self.infer_series_dir(file_name))

    @staticmethod
    def download_file(url: str, file_path: Path) -> str:
        if file_path.exists():
            return "exists"
        response = requests.get(url, timeout=(10, 60))
        response.raise_for_status()
        file_path.write_bytes(response.content)
        return "created"

    def touch_strm_file(
        self,
        storage_path: Path,
        file_name: str,
        file_url: str,
        relative_dir: Optional[str] = None,
        season: Optional[str] = None,
    ) -> str:
        if not storage_path:
            logger.error("创建 strm 源文件失败：未配置存储目录")
            return "failed"

        directory = self.build_directory(storage_path, file_name, relative_dir)
        extension = self.file_extension(file_name)

        try:
            directory.mkdir(parents=True, exist_ok=True)
            if extension in self.VIDEO_EXTENSIONS:
                src_url = AniStrmClient.normalize_stream_url(file_url)
                file_path = directory / f"{self.safe_path_part(file_name)}.strm"
                if file_path.exists():
                    logger.debug("ANi-Strm 跳过已存在文件：%s", file_path.name)
                    return "exists"
                file_path.write_text(src_url, encoding="utf-8")
                logger.info("创建 STRM：%s", file_path.relative_to(storage_path))
                return "created"

            if extension in self.DIRECT_DOWNLOAD_EXTENSIONS:
                file_path = directory / self.safe_path_part(file_name)
                status = self.download_file(file_url, file_path)
                if status == "created":
                    logger.info("下载附属文件：%s", file_path.relative_to(storage_path))
                return status

            logger.info("跳过非视频文件：%s", file_name)
            return "exists"
        except Exception as exc:
            logger.error("处理文件失败：%s - %s", file_name, exc)
            return "failed"


class AppService:
    def __init__(self):
        self.lock = threading.Lock()
        self.state = StateModel()
        self.scheduler = BackgroundScheduler(timezone=TZ_NAME)
        self.config = self.load_config()
        self.strm_service = StrmFileService()
        self.scheduler.start()
        self.reschedule()

    def load_config(self) -> ConfigModel:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not CONFIG_PATH.exists():
            config = ConfigModel(**DEFAULT_CONFIG)
            self.save_config(config)
            return config
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        return ConfigModel(**{**DEFAULT_CONFIG, **data, "output_dir": str(OUTPUT_DIR)})

    def save_config(self, config: ConfigModel) -> None:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(config.model_dump(), ensure_ascii=False, indent=2), encoding="utf-8")

    def update_config(self, config: ConfigModel) -> ConfigModel:
        self.config = config
        self.save_config(config)
        self.reschedule()
        return self.config

    def reschedule(self) -> None:
        self.scheduler.remove_all_jobs()
        if self.config.enabled:
            self.scheduler.add_job(
                self.run_incremental,
                trigger=CronTrigger.from_crontab(self.config.cron, timezone=TZ_NAME),
                id="anistrm_incremental",
                name="ANiStrm 增量更新",
                replace_existing=True,
                max_instances=1,
            )
            logger.info("定时任务已启用：%s", self.config.cron)
        else:
            logger.info("定时任务已停用")

    def build_client(self) -> AniStrmClient:
        return AniStrmClient(
            proxy_base=self.config.proxy_base,
            use_proxy=self.config.use_proxy,
            http_proxy=self.config.http_proxy,
        )

    def get_target_seasons(self, client: AniStrmClient) -> List[str]:
        if self.config.full_sync_once:
            seasons = client.get_available_seasons()
            if seasons:
                logger.info("已启用一次性全量生成，将处理全部季度：%s", seasons)
                return seasons
            logger.warning("一次性全量生成未获取到季度列表，将回退到当前季度选择")

        seasons: List[str] = []
        for season in self.config.selected_seasons:
            if season == "latest":
                latest = client.get_current_season()
                if latest:
                    seasons.append(latest)
            else:
                seasons.append(season)
        return list(dict.fromkeys(seasons))

    def run_incremental(self) -> RunResult:
        if not self.lock.acquire(blocking=False):
            raise RuntimeError("已有任务正在运行")

        started_at = datetime.now(TZ).isoformat(timespec="seconds")
        self.state.running = True
        self.state.last_run_started_at = started_at
        self.state.last_error = None
        try:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            client = self.build_client()
            seasons = self.get_target_seasons(client)
            if not seasons:
                logger.info("未选择任何季度，任务结束")

            logger.info("ANi-Strm 任务开始：seasons=%s storage=%s", seasons, OUTPUT_DIR)
            total_files = total_created = total_exists = total_failed = 0

            for season in seasons:
                file_entries = client.get_season_entries(season)
                season_total = len(file_entries)
                season_created = season_exists = season_failed = 0
                logger.info("开始处理季度：%s，文件数=%s", season, season_total)

                for file_entry in file_entries:
                    status = self.strm_service.touch_strm_file(
                        storage_path=OUTPUT_DIR,
                        file_name=file_entry["name"],
                        file_url=file_entry["url"],
                        relative_dir=file_entry.get("relative_dir"),
                        season=file_entry.get("season") or season,
                    )
                    if status == "created":
                        season_created += 1
                    elif status == "exists":
                        season_exists += 1
                    else:
                        season_failed += 1

                total_files += season_total
                total_created += season_created
                total_exists += season_exists
                total_failed += season_failed
                logger.info(
                    "季度处理完成：%s，总数=%s，新增=%s，跳过=%s，失败=%s",
                    season,
                    season_total,
                    season_created,
                    season_exists,
                    season_failed,
                )

            finished_at = datetime.now(TZ).isoformat(timespec="seconds")
            result = RunResult(
                seasons=seasons,
                total_files=total_files,
                created=total_created,
                exists=total_exists,
                failed=total_failed,
                started_at=started_at,
                finished_at=finished_at,
            )
            self.state.last_result = result
            self.state.last_run_finished_at = finished_at
            if self.config.full_sync_once:
                self.config.full_sync_once = False
                self.save_config(self.config)
                logger.info("一次性全量生成已完成，开关已自动关闭")
            logger.info(
                "ANi-Strm 任务完成：季度数=%s，文件总数=%s，新增=%s，跳过=%s，失败=%s",
                len(seasons),
                total_files,
                total_created,
                total_exists,
                total_failed,
            )
            return result
        except Exception as exc:
            self.state.last_error = str(exc)
            logger.exception("ANi-Strm 任务失败")
            raise
        finally:
            self.state.running = False
            self.lock.release()

    def state_with_schedule(self) -> StateModel:
        state = self.state.model_copy(deep=True)
        job = self.scheduler.get_job("anistrm_incremental")
        if job and job.next_run_time:
            state.next_run_at = job.next_run_time.astimezone(TZ).isoformat(timespec="seconds")
        return state


service = AppService()
app = FastAPI(title="ANiStrm Standalone", version="1.0.0")


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return INDEX_HTML


@app.get("/api/config")
def get_config() -> Dict[str, Any]:
    return service.config.model_dump()


@app.put("/api/config")
def put_config(config: ConfigModel) -> Dict[str, Any]:
    return service.update_config(config).model_dump()


@app.get("/api/state")
def get_state() -> Dict[str, Any]:
    return service.state_with_schedule().model_dump()


@app.get("/api/logs")
def get_logs(limit: int = 120) -> Dict[str, Any]:
    limit = max(1, min(limit, 300))
    return {"logs": memory_handler.tail(limit)}


@app.get("/api/seasons")
def get_seasons() -> Dict[str, Any]:
    client = service.build_client()
    seasons = client.get_available_seasons()
    local = client._get_local_season()
    if local not in seasons:
        seasons.append(local)
    return {"seasons": seasons}


@app.post("/api/run")
def run_now() -> Dict[str, Any]:
    try:
        result = service.run_incremental()
        return result.model_dump()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ANiStrm</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f8;
      --panel: #ffffff;
      --text: #202124;
      --muted: #687076;
      --line: #dfe4e8;
      --accent: #0f766e;
      --accent-dark: #115e59;
      --danger: #b42318;
      --warn: #a16207;
      --ok: #0f766e;
      --shadow: 0 10px 30px rgba(25, 35, 45, .08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      border-bottom: 1px solid var(--line);
      background: rgba(255,255,255,.9);
      position: sticky;
      top: 0;
      z-index: 10;
      backdrop-filter: blur(14px);
    }
    .bar {
      width: min(1180px, calc(100vw - 32px));
      margin: 0 auto;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      min-height: 68px;
    }
    h1 {
      margin: 0;
      font-size: 22px;
      line-height: 1.2;
      letter-spacing: 0;
    }
    .sub {
      margin-top: 4px;
      color: var(--muted);
      font-size: 13px;
    }
    main {
      width: min(1180px, calc(100vw - 32px));
      margin: 24px auto 40px;
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(320px, .9fr);
      gap: 18px;
    }
    section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .section-head {
      padding: 18px 20px 12px;
      border-bottom: 1px solid var(--line);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    h2 {
      margin: 0;
      font-size: 16px;
      line-height: 1.3;
      letter-spacing: 0;
    }
    form {
      padding: 18px 20px 20px;
      display: grid;
      gap: 16px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
    }
    label {
      display: grid;
      gap: 7px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.2;
    }
    input, select {
      width: 100%;
      min-height: 40px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px 10px;
      font: inherit;
      color: var(--text);
      background: #fff;
      outline: none;
    }
    select[multiple] {
      min-height: 178px;
    }
    input:focus, select:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, .12);
    }
    .switch-row {
      display: flex;
      align-items: center;
      gap: 18px;
      flex-wrap: wrap;
    }
    .switch {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--text);
      font-size: 14px;
    }
    .switch input {
      width: 18px;
      height: 18px;
      min-height: 18px;
      accent-color: var(--accent);
    }
    .actions {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    button {
      min-height: 38px;
      border: 1px solid transparent;
      border-radius: 6px;
      padding: 8px 13px;
      font: inherit;
      font-weight: 650;
      cursor: pointer;
      background: var(--accent);
      color: white;
    }
    button:hover { background: var(--accent-dark); }
    button.secondary {
      background: #fff;
      color: var(--text);
      border-color: var(--line);
    }
    button.secondary:hover { background: #f4f6f7; }
    button:disabled {
      opacity: .55;
      cursor: not-allowed;
    }
    .status {
      padding: 18px 20px;
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .metric {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 13px;
      min-height: 74px;
      background: #fbfcfc;
    }
    .metric .label {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 7px;
    }
    .metric .value {
      font-size: 18px;
      font-weight: 720;
      overflow-wrap: anywhere;
    }
    .full { grid-column: 1 / -1; }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 26px;
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 12px;
      font-weight: 700;
      background: #e8f5f3;
      color: var(--ok);
    }
    .pill.warn { background: #fff7ed; color: var(--warn); }
    .pill.danger { background: #fef3f2; color: var(--danger); }
    pre {
      margin: 0;
      padding: 16px 20px 20px;
      min-height: 360px;
      max-height: 560px;
      overflow: auto;
      border-top: 1px solid var(--line);
      background: #111827;
      color: #d1d5db;
      font: 12px/1.55 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }
    .message {
      color: var(--muted);
      font-size: 13px;
      min-height: 18px;
    }
    @media (max-width: 860px) {
      main { grid-template-columns: 1fr; }
      .grid, .status { grid-template-columns: 1fr; }
      .bar { align-items: flex-start; flex-direction: column; padding: 14px 0; }
    }
  </style>
</head>
<body>
  <header>
    <div class="bar">
      <div>
        <h1>ANiStrm</h1>
        <div class="sub">Open ANi STRM 增量生成器</div>
      </div>
      <div id="runningPill" class="pill">加载中</div>
    </div>
  </header>
  <main>
    <section>
      <div class="section-head">
        <h2>设置</h2>
        <button class="secondary" type="button" id="refreshSeasons">刷新季度</button>
      </div>
      <form id="configForm">
        <div class="switch-row">
          <label class="switch"><input id="enabled" type="checkbox" />启用定时任务</label>
          <label class="switch"><input id="fullSyncOnce" type="checkbox" />下次全量生成</label>
          <label class="switch"><input id="useProxy" type="checkbox" />使用 HTTP 代理</label>
        </div>
        <div class="grid">
          <label>执行周期
            <input id="cron" placeholder="20 22,23,0,1 * * *" />
          </label>
          <label>输出目录
            <input id="outputDir" disabled />
          </label>
        </div>
        <label>Open ANi 反代地址
          <input id="proxyBase" placeholder="https://openani.an-i.workers.dev" />
        </label>
        <label>HTTP 代理地址
          <input id="httpProxy" placeholder="http://host:port 或 socks5://host:port" />
        </label>
        <label>拉取季度
          <select id="selectedSeasons" multiple></select>
        </label>
        <div class="actions">
          <button type="submit">保存设置</button>
          <button type="button" id="runNow">立即增量更新</button>
          <button class="secondary" type="button" id="refreshAll">刷新状态</button>
        </div>
        <div class="message" id="message"></div>
      </form>
    </section>
    <section>
      <div class="section-head">
        <h2>状态</h2>
        <span id="nextRun" class="pill">未调度</span>
      </div>
      <div class="status">
        <div class="metric"><div class="label">总文件</div><div class="value" id="totalFiles">-</div></div>
        <div class="metric"><div class="label">新增</div><div class="value" id="created">-</div></div>
        <div class="metric"><div class="label">已存在</div><div class="value" id="exists">-</div></div>
        <div class="metric"><div class="label">失败</div><div class="value" id="failed">-</div></div>
        <div class="metric full"><div class="label">最近季度</div><div class="value" id="seasons">-</div></div>
        <div class="metric full"><div class="label">最近运行</div><div class="value" id="lastRun">-</div></div>
      </div>
      <pre id="logs">加载日志中...</pre>
    </section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    let currentConfig = null;

    async function request(path, options = {}) {
      const response = await fetch(path, {
        headers: { "Content-Type": "application/json" },
        ...options
      });
      if (!response.ok) {
        let detail = response.statusText;
        try {
          const payload = await response.json();
          detail = payload.detail || JSON.stringify(payload);
        } catch (_) {}
        throw new Error(detail);
      }
      return response.json();
    }

    function setMessage(text, danger = false) {
      $("message").textContent = text;
      $("message").style.color = danger ? "var(--danger)" : "var(--muted)";
    }

    function setSelected(values) {
      const wanted = new Set(values || []);
      Array.from($("selectedSeasons").options).forEach((option) => {
        option.selected = wanted.has(option.value);
      });
    }

    function selectedValues() {
      return Array.from($("selectedSeasons").selectedOptions).map((option) => option.value);
    }

    async function loadConfig() {
      currentConfig = await request("/api/config");
      $("enabled").checked = currentConfig.enabled;
      $("fullSyncOnce").checked = currentConfig.full_sync_once;
      $("useProxy").checked = currentConfig.use_proxy;
      $("cron").value = currentConfig.cron;
      $("proxyBase").value = currentConfig.proxy_base;
      $("httpProxy").value = currentConfig.http_proxy || "";
      $("outputDir").value = currentConfig.output_dir;
      await loadSeasons(currentConfig.selected_seasons);
    }

    async function loadSeasons(selected = null) {
      const select = $("selectedSeasons");
      const previous = selected || selectedValues();
      select.innerHTML = "";
      const latest = document.createElement("option");
      latest.value = "latest";
      latest.textContent = "最新季度";
      select.appendChild(latest);
      try {
        const data = await request("/api/seasons");
        data.seasons.forEach((season) => {
          if (season === "latest") return;
          const option = document.createElement("option");
          option.value = season;
          option.textContent = season;
          select.appendChild(option);
        });
      } catch (error) {
        setMessage("季度列表获取失败：" + error.message, true);
      }
      setSelected(previous && previous.length ? previous : ["latest"]);
    }

    async function saveConfig(event) {
      event.preventDefault();
      const payload = {
        enabled: $("enabled").checked,
        full_sync_once: $("fullSyncOnce").checked,
        use_proxy: $("useProxy").checked,
        cron: $("cron").value.trim(),
        proxy_base: $("proxyBase").value.trim(),
        http_proxy: $("httpProxy").value.trim(),
        selected_seasons: selectedValues(),
        output_dir: $("outputDir").value
      };
      try {
        currentConfig = await request("/api/config", {
          method: "PUT",
          body: JSON.stringify(payload)
        });
        setMessage("设置已保存");
        await loadState();
      } catch (error) {
        setMessage("保存失败：" + error.message, true);
      }
    }

    async function runNow() {
      $("runNow").disabled = true;
      setMessage("正在执行增量更新...");
      try {
        await request("/api/run", { method: "POST" });
        setMessage("增量更新完成");
      } catch (error) {
        setMessage("运行失败：" + error.message, true);
      } finally {
        $("runNow").disabled = false;
        await refreshAll();
      }
    }

    async function loadState() {
      const state = await request("/api/state");
      const result = state.last_result || {};
      $("runningPill").textContent = state.running ? "运行中" : "空闲";
      $("runningPill").className = "pill" + (state.running ? " warn" : "");
      $("nextRun").textContent = state.next_run_at ? "下次 " + state.next_run_at.replace("T", " ") : "未调度";
      $("nextRun").className = "pill" + (state.next_run_at ? "" : " warn");
      $("totalFiles").textContent = result.total_files ?? "-";
      $("created").textContent = result.created ?? "-";
      $("exists").textContent = result.exists ?? "-";
      $("failed").textContent = result.failed ?? "-";
      $("seasons").textContent = result.seasons ? result.seasons.join(", ") : "-";
      $("lastRun").textContent = result.finished_at ? result.finished_at.replace("T", " ") : "-";
      if (state.last_error) setMessage("最近错误：" + state.last_error, true);
    }

    async function loadLogs() {
      const data = await request("/api/logs?limit=180");
      $("logs").textContent = data.logs.join("\n") || "暂无日志";
    }

    async function refreshAll() {
      await Promise.all([loadState(), loadLogs()]);
    }

    $("configForm").addEventListener("submit", saveConfig);
    $("runNow").addEventListener("click", runNow);
    $("refreshAll").addEventListener("click", refreshAll);
    $("refreshSeasons").addEventListener("click", () => loadSeasons());

    loadConfig().then(refreshAll).catch((error) => {
      setMessage("加载失败：" + error.message, true);
    });
    setInterval(refreshAll, 5000);
  </script>
</body>
</html>
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import quote


WORKSPACE_NAME = Path(__file__).resolve().parent.name


def _candidate_secret_files(workspace_name: str = WORKSPACE_NAME) -> list[Path]:
    home = Path.home()
    return [
        home / "secrets" / workspace_name / ".json",
        home / "secrets" / f"{workspace_name}.json",
        home / "secrets" / workspace_name / "secrets.json",
    ]


def _apply_env_mapping(mapping: dict) -> None:
    sections = []
    for key in ("common", "prod"):
        value = mapping.get(key)
        if isinstance(value, dict):
            sections.append(value)

    top_level = {k: v for k, v in mapping.items() if k not in {"common", "prod"}}
    sections.append(top_level)

    for section in sections:
        for key, value in section.items():
            if value is None:
                continue
            if not os.getenv(str(key)):
                os.environ[str(key)] = str(value).strip("'\"")


def load_workspace_secret_env_defaults(workspace_name: str = WORKSPACE_NAME) -> Path | None:
    """Load env defaults from ~/secrets/<workspace_name>/.json if available."""
    override = os.getenv("WORKSPACE_SECRET_FILE")
    candidates = [Path(override)] if override else []
    candidates.extend(_candidate_secret_files(workspace_name))

    for path in candidates:
        try:
            if not path.exists():
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                _apply_env_mapping(payload)
                return path
        except Exception:
            continue
    return None


def build_postgres_dsn(default_db: str = "ssh_reports_hub") -> str:
    url = os.getenv("POSTGRES_URL")
    if url:
        return url

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", default_db)
    user = os.getenv("POSTGRES_USER", "ssh_reports_hub")
    password = quote(os.getenv("POSTGRES_PASSWORD", ""), safe="")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


load_workspace_secret_env_defaults()

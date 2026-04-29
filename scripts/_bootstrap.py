from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from secret_env import build_postgres_dsn, load_workspace_secret_env_defaults

load_workspace_secret_env_defaults()

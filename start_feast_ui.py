"""Launch Feast UI from the initialized feature repository."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

REPO_CANDIDATES = [
    Path.home() / "feast_project" / "feature_repo",
    Path.home() / "feast_repo",
]


def find_feature_repo() -> Path:
    """Return the first candidate path that contains `feature_store.yaml`."""
    errors: list[str] = []
    for candidate in REPO_CANDIDATES:
        if not candidate.exists():
            errors.append(f"{candidate}: directory not found")
            continue
        if (candidate / "feature_store.yaml").exists():
            return candidate
        errors.append(f"{candidate}: missing feature_store.yaml")
    joined = "; ".join(errors)
    raise FileNotFoundError(
        "Could not locate a Feast repository with feature_store.yaml. "
        f"Tried: {joined}"
    )


def start_feast_ui() -> None:
    """Run `feast ui` from the validated repository path."""
    repo_path = find_feature_repo()
    env = os.environ.copy()
    env.setdefault("FEAST_PROJECT", "feast_demo")
    port = env.get("CDSW_APP_PORT", "8000")
    subprocess.run(
        ["feast", "ui", "--host", "localhost", "--port", port],
        cwd=str(repo_path),
        env=env,
        check=True,
    )


if __name__ == "__main__":
    start_feast_ui()

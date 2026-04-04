#!/usr/bin/env python3
"""
Smoke-test for dataset repo commit + push.

Writes a small marker file, commits, pushes, and verifies the remote
received the commit.  Does NOT touch Snowflake or re-export any data.

Usage:
    python scripts/test_push.py
    python scripts/test_push.py --dry-run   # commit only, skip push
"""
import sys
import subprocess
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))

from pipeline.config.settings import get_settings
from pipeline.utils.repo_manager import DatasetRepoManager


def main():
    dry_run = "--dry-run" in sys.argv
    settings = get_settings()

    mgr = DatasetRepoManager(
        repo_dir=settings.export_base_dir,
        remote_url=settings.dataset_repo_url,
    )

    print(f"Repo dir : {mgr.repo_dir.resolve()}")
    print(f"Remote   : {mgr.remote_url or '(none)'}")
    print(f"Mode     : {'dry-run (commit only)' if dry_run else 'commit + push'}")
    print()

    mgr.ensure_init()
    print("[ok] ensure_init")

    # 2. write a small marker so there's always something to commit
    marker = mgr.repo_dir / ".push_test"
    marker.write_text(
        f"push test at {datetime.now().astimezone().isoformat()}\n",
        encoding="utf-8",
    )
    print(f"[ok] wrote {marker.name}")

    # 3. commit
    sha = mgr.commit(f"test-push [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
    if sha:
        print(f"[ok] committed {sha[:8]}")
    else:
        print("[!!] nothing to commit — repo was already clean")
        return

    # 4. push (unless --dry-run)
    if dry_run:
        print("[--] skipping push (dry-run)")
        return

    if not settings.dataset_repo_url:
        print("[!!] DATASET_REPO_URL not set, cannot push")
        return

    push_info = mgr.push()
    print(f"[ok] pushed {push_info['branch']} @ {push_info['head']}")

    # 5. verify: ls-remote should show the same SHA on the default branch
    result = subprocess.run(
        ["git", "ls-remote", "origin", f"refs/heads/{push_info['branch']}"],
        cwd=str(mgr.repo_dir),
        capture_output=True, text=True,
    )
    remote_sha = result.stdout.split()[0] if result.stdout.strip() else "(not found)"
    local_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(mgr.repo_dir),
        capture_output=True, text=True,
    ).stdout.strip()

    match = remote_sha == local_sha
    status = "PASS" if match else "FAIL"
    print(f"\n  local  HEAD = {local_sha}")
    print(f"  remote HEAD = {remote_sha}")
    print(f"  [{status}] {'remote matches local' if match else 'MISMATCH — push may have failed'}")


if __name__ == "__main__":
    main()

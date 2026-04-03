"""
Dataset Repo Manager
Manages a single disposable Git repository for data delivery.

Architecture:
  Every run completely resets the repository — no history is preserved.
  The repo acts as a fresh data delivery envelope containing:
  - Encrypted data files (Parquet chunks per table)
  - A delivery manifest with everything the consumer needs to import

Two transport modes:
  1. **Remote push/pull** — force-push to a shared Git remote from the
     VPN side, shallow-clone on the PSQL side.
  2. **Git bundles** — ``git bundle create`` for truly air-gapped transfer.

Producer (VPN/Snowflake side)::

    mgr = DatasetRepoManager("repos/dataset", "git@github.com:org/ms-data.git")
    mgr.reset()
    mgr.stage_table(Path("exports/abc123"), "abc123")
    mgr.write_delivery_manifest(manifest, password="...")
    mgr.commit("upsert: 14 tables [2026-04-03 14:30]")
    mgr.push()

Consumer (PostgreSQL side)::

    mgr = DatasetRepoManager("repos/dataset", "git@github.com:org/ms-data.git")
    mgr.pull()
    manifest = mgr.read_delivery_manifest(password="...")
    # manifest["tables"] has per-table sync_mode, merge_keys, postgres targets
"""
import json
import os
import stat
import subprocess
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


def _force_remove_readonly(func, path, exc_info):
    """Handle read-only files on Windows (e.g. .git/objects/)."""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def _run_git(args: List[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess:
    """Run a git command and return the result."""
    cmd = ["git"] + args
    logger.debug(f"git {' '.join(args)}  (cwd={cwd})")
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        logger.error(f"git command failed: {result.stderr.strip()}")
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)
    return result


class DatasetRepoManager:
    """
    Manage a single disposable Git repository for data delivery.

    Every run resets the repo completely — no history is preserved.
    The repo is a fresh envelope containing encrypted data files and
    a delivery manifest with import instructions.

    The delivery manifest (``delivery_manifest.json`` or ``.enc``)
    contains everything the consumer needs: per-table sync modes,
    merge keys, PostgreSQL targets, and watermark state.
    """

    MANIFEST_PLAIN = "delivery_manifest.json"
    MANIFEST_ENCRYPTED = "delivery_manifest.enc"

    def __init__(self, repo_dir: str, remote_url: Optional[str] = None):
        self.repo_dir = Path(repo_dir)
        self.remote_url = remote_url

    # ------------------------------------------------------------------ reset
    def reset(self):
        """
        Nuke the repo entirely and ``git init`` fresh.

        Removes all files (including ``.git``) so the next commit
        has no parent — the repo starts completely empty.
        """
        if self.repo_dir.exists():
            shutil.rmtree(str(self.repo_dir), onerror=_force_remove_readonly)
            logger.info(f"Removed existing repo: {self.repo_dir}")

        self.repo_dir.mkdir(parents=True, exist_ok=True)
        _run_git(["init"], cwd=self.repo_dir)
        logger.info(f"Initialised fresh repo: {self.repo_dir}")

    # --------------------------------------------------------------- staging
    def stage_table(self, source_dir: Path, dest_folder: str):
        """
        Copy a table's export folder into the repo.

        Args:
            source_dir: Path to the export folder (e.g. ``exports/<folder_id>``).
            dest_folder: Folder name to use inside the repo.
        """
        source_dir = Path(source_dir)
        if not source_dir.is_dir():
            raise FileNotFoundError(f"Export directory not found: {source_dir}")

        dest = self.repo_dir / dest_folder
        if dest.exists():
            shutil.rmtree(str(dest))
        shutil.copytree(str(source_dir), str(dest))
        logger.info(f"Staged {dest_folder} into dataset repo")

    # ------------------------------------------------------------ manifest
    def write_delivery_manifest(
        self,
        manifest: Dict[str, Any],
        password: Optional[str] = None,
    ):
        """
        Write the delivery manifest into the repo root.

        If *password* is provided the manifest is encrypted as
        ``delivery_manifest.enc``; otherwise it is written as
        plain ``delivery_manifest.json``.

        The delivery manifest contains everything the consumer needs
        to import the data: per-table sync modes, merge keys,
        PostgreSQL targets, and watermark state.
        """
        manifest_json = json.dumps(manifest, indent=2, default=str)

        if password:
            from pipeline.transformers.encryptor import FileEncryptor

            temp_path = self.repo_dir / "_manifest_tmp.json"
            temp_path.write_text(manifest_json, encoding="utf-8")

            enc_path = self.repo_dir / self.MANIFEST_ENCRYPTED
            encryptor = FileEncryptor()
            encryptor.encrypt_file(temp_path, enc_path, password)
            temp_path.unlink()

            logger.info(f"Delivery manifest written (encrypted): {enc_path}")
        else:
            plain_path = self.repo_dir / self.MANIFEST_PLAIN
            plain_path.write_text(manifest_json, encoding="utf-8")
            logger.info(f"Delivery manifest written: {plain_path}")

    def read_delivery_manifest(self, password: Optional[str] = None) -> Dict[str, Any]:
        """
        Read and parse the delivery manifest from the repo.

        Tries plain JSON first, then encrypted.
        """
        plain_path = self.repo_dir / self.MANIFEST_PLAIN
        enc_path = self.repo_dir / self.MANIFEST_ENCRYPTED

        if plain_path.exists():
            with open(plain_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            logger.info("Read delivery manifest (plain)")
            return manifest

        if enc_path.exists():
            if not password:
                raise ValueError(
                    "Delivery manifest is encrypted but no password provided. "
                    "Set ENCRYPTION_PASSWORD in .env"
                )
            from pipeline.transformers.encryptor import FileEncryptor
            import os

            encryptor = FileEncryptor()
            fd, tmp_name = tempfile.mkstemp(suffix=".json")
            temp_path = Path(tmp_name)
            try:
                os.close(fd)
                encryptor.decrypt_file(enc_path, temp_path, password)
                with open(temp_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
            finally:
                if temp_path.exists():
                    temp_path.unlink()

            logger.info("Read delivery manifest (encrypted)")
            return manifest

        raise FileNotFoundError(
            f"Delivery manifest not found in {self.repo_dir}. "
            f"Looked for: {self.MANIFEST_PLAIN}, {self.MANIFEST_ENCRYPTED}"
        )

    # ------------------------------------------------------------- committing
    def commit(self, message: str) -> Optional[str]:
        """
        Stage all files and commit.

        Returns the commit SHA, or None if nothing to commit.
        """
        result = _run_git(["status", "--porcelain"], cwd=self.repo_dir)
        if not result.stdout.strip():
            logger.info("Nothing to commit")
            return None

        _run_git(["add", "."], cwd=self.repo_dir)
        _run_git(["commit", "-m", message], cwd=self.repo_dir)

        sha = _run_git(["rev-parse", "HEAD"], cwd=self.repo_dir).stdout.strip()
        logger.info(f"Committed {sha[:8]}: {message}")
        return sha

    # --------------------------------------------------------- remote push/pull
    def push(self, remote_name: str = "origin", force: bool = True) -> Dict[str, Any]:
        """
        Force-push to the configured remote.

        Since the repo is reset each run, force-push is required
        (the local history is always unrelated to the remote).
        """
        if not self.remote_url:
            raise ValueError("No remote URL configured (set DATASET_REPO_URL in .env)")

        result = _run_git(["remote"], cwd=self.repo_dir)
        remotes = result.stdout.strip().splitlines()
        if remote_name in remotes:
            _run_git(["remote", "set-url", remote_name, self.remote_url], cwd=self.repo_dir)
        else:
            _run_git(["remote", "add", remote_name, self.remote_url], cwd=self.repo_dir)

        branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=self.repo_dir).stdout.strip()

        args = ["push", "-u", remote_name, branch]
        if force:
            args.insert(1, "--force")

        _run_git(args, cwd=self.repo_dir)

        sha = _run_git(["rev-parse", "--short", "HEAD"], cwd=self.repo_dir).stdout.strip()
        logger.info(f"Pushed dataset repo ({branch} @ {sha}) to {remote_name}")

        return {"branch": branch, "head": sha, "remote": remote_name}

    def pull(self, remote_name: str = "origin") -> Dict[str, Any]:
        """
        Pull the latest delivery from the remote.

        Since the producer resets the repo each run (new root commit
        every time), the consumer also does a fresh shallow clone.
        If a local copy exists it is deleted first.
        """
        if not self.remote_url:
            raise ValueError("No remote URL configured (set DATASET_REPO_URL in .env)")

        if self.repo_dir.exists():
            shutil.rmtree(str(self.repo_dir), onerror=_force_remove_readonly)
            logger.info(f"Removed existing local repo: {self.repo_dir}")

        self.repo_dir.parent.mkdir(parents=True, exist_ok=True)
        _run_git(
            ["clone", "--depth", "1", self.remote_url, str(self.repo_dir.resolve())],
            cwd=self.repo_dir.parent,
        )

        sha = _run_git(["rev-parse", "--short", "HEAD"], cwd=self.repo_dir).stdout.strip()
        branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=self.repo_dir).stdout.strip()
        commit_msg = _run_git(["log", "-1", "--format=%s"], cwd=self.repo_dir).stdout.strip()

        logger.info(f"Pulled dataset repo: {branch} @ {sha}")

        return {
            "target_dir": str(self.repo_dir),
            "branch": branch,
            "head": sha,
            "commit_message": commit_msg,
        }

    # ---------------------------------------------------------------- bundles
    def create_bundle(self, output_path: Path, ref: str = "HEAD") -> Dict[str, Any]:
        """Create a ``git bundle`` for air-gapped transfer."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        _run_git(["bundle", "create", str(output_path.resolve()), ref], cwd=self.repo_dir)

        size = output_path.stat().st_size
        logger.info(f"Created bundle: {output_path}  ({size / (1024*1024):.2f} MB)")

        return {
            "bundle_path": str(output_path),
            "size_bytes": size,
            "size_mb": size / (1024 * 1024),
        }

    @staticmethod
    def apply_bundle(bundle_path: Path, target_dir: Path) -> Dict[str, Any]:
        """
        Apply a ``git bundle`` on the consumer side.

        Since each bundle represents a complete single-commit repo,
        this always clones fresh from the bundle.
        """
        bundle_path = Path(bundle_path).resolve()
        target_dir = Path(target_dir)

        if not bundle_path.is_file():
            raise FileNotFoundError(f"Bundle not found: {bundle_path}")

        if target_dir.exists():
            shutil.rmtree(str(target_dir), onerror=_force_remove_readonly)

        target_dir.parent.mkdir(parents=True, exist_ok=True)
        _run_git(
            ["clone", str(bundle_path), str(target_dir.resolve())],
            cwd=target_dir.parent,
        )

        sha = _run_git(["rev-parse", "HEAD"], cwd=target_dir).stdout.strip()
        branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=target_dir).stdout.strip()

        logger.info(f"Applied bundle to {target_dir}")

        return {
            "target_dir": str(target_dir),
            "branch": branch,
            "head_sha": sha,
        }

    # -------------------------------------------------------------- info / status
    def status(self) -> Dict[str, Any]:
        """Return a summary of the repo state."""
        git_dir = self.repo_dir / ".git"
        if not git_dir.exists():
            return {"initialised": False, "path": str(self.repo_dir)}

        branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=self.repo_dir).stdout.strip()
        sha = _run_git(["rev-parse", "--short", "HEAD"], cwd=self.repo_dir).stdout.strip()
        log_line = _run_git(["log", "-1", "--format=%s (%ar)"], cwd=self.repo_dir).stdout.strip()

        has_manifest = (
            (self.repo_dir / self.MANIFEST_PLAIN).exists()
            or (self.repo_dir / self.MANIFEST_ENCRYPTED).exists()
        )

        count_result = _run_git(["count-objects", "-v"], cwd=self.repo_dir)
        size_kb = 0
        for line in count_result.stdout.splitlines():
            if line.startswith("size-pack:"):
                size_kb = int(line.split(":")[1].strip())

        return {
            "initialised": True,
            "path": str(self.repo_dir),
            "branch": branch,
            "head": sha,
            "last_commit": log_line,
            "has_manifest": has_manifest,
            "pack_size_mb": size_kb / 1024,
        }


# ================================================================== helpers
def build_delivery_manifest(
    export_results: List[Dict[str, Any]],
    table_configs: List[Dict[str, Any]],
    run_purpose: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a delivery manifest from export results and table configs.

    The manifest contains everything the consumer needs to import:
    per-table sync modes, merge keys, PostgreSQL targets, watermark state.
    """
    timestamp = datetime.now().astimezone().isoformat()
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    if not run_purpose:
        run_purpose = _auto_purpose(export_results)

    config_lookup = {tc["name"]: tc for tc in table_configs}

    tables = []
    for result in export_results:
        name = result["table_name"]
        tc = config_lookup.get(name, {})

        tables.append({
            "name": name,
            "sync_mode": result.get("sync_mode", tc.get("sync_mode", "full")),
            "merge_keys": tc.get("merge_keys", []),
            "watermark_column": tc.get("watermark_column"),
            "total_rows": result.get("total_rows", 0),
            "total_chunks": result.get("total_chunks", 0),
            "data_folder": result.get("folder_id") or name,
            "postgres": tc.get("postgres", {}),
        })

    return {
        "version": 1,
        "run_id": run_id,
        "run_timestamp": timestamp,
        "run_purpose": run_purpose,
        "tables": tables,
    }


def _auto_purpose(export_results: List[Dict[str, Any]]) -> str:
    """Generate a human-readable run purpose from export results."""
    modes: Dict[str, List[str]] = {}
    for r in export_results:
        mode = r.get("sync_mode", "full")
        modes.setdefault(mode, []).append(r["table_name"])

    parts = []
    for mode, tables in sorted(modes.items()):
        if len(tables) <= 3:
            parts.append(f"{mode}: {', '.join(tables)}")
        else:
            parts.append(f"{mode}: {len(tables)} tables")
    return "; ".join(parts)

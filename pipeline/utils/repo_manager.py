"""
Git Repo Manager
Manages seed / delta repository topology for Git-based data delivery.

Architecture:
  - **Seed repo**  – contains initial full-load exports, cloned once by
    the consumer and rarely updated afterwards.
  - **Delta repo** – contains incremental / upsert exports that arrive
    on each sync cycle.  Supports orphan branches to keep history light.

Two transport modes:
  1. **Remote push/pull** – push to a shared Git remote (GitHub, GitLab,
     internal server) from the VPN side, pull on the PSQL side.
  2. **Git bundles** – ``git bundle create`` for truly air-gapped transfer.
"""
import json
import subprocess
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


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


class GitRepoManager:
    """
    Manage seed and delta Git repositories for data delivery.

    **Producer (VPN side) — push workflow**::

        mgr = GitRepoManager(
            seed_dir="repos/seed", delta_dir="repos/delta",
            seed_url="git@github.com:org/ms-data-seed.git",
            delta_url="git@github.com:org/ms-data-delta.git",
        )
        mgr.init_repos()

        mgr.stage_table("exports/TABLE_A", "TABLE_A", sync_mode="full")
        mgr.commit_seed("seed 2026-04-02: TABLE_A")
        mgr.push("seed")

        mgr.stage_table("exports/TABLE_B", "TABLE_B", sync_mode="upsert")
        mgr.commit_delta("delta 2026-04-02: TABLE_B")
        mgr.push("delta")

    **Consumer (PSQL side) — pull workflow**::

        mgr = GitRepoManager(
            seed_dir="repos/seed", delta_dir="repos/delta",
            seed_url="git@github.com:org/ms-data-seed.git",
            delta_url="git@github.com:org/ms-data-delta.git",
        )
        mgr.pull("seed")    # clone on first run, pull on subsequent
        mgr.pull("delta")
        # Then import from repos/seed/ and repos/delta/
    """

    def __init__(
        self,
        seed_dir: str = "repos/seed",
        delta_dir: str = "repos/delta",
        seed_url: Optional[str] = None,
        delta_url: Optional[str] = None,
    ):
        self.seed_dir = Path(seed_dir)
        self.delta_dir = Path(delta_dir)
        self._urls: Dict[str, Optional[str]] = {
            "seed": seed_url,
            "delta": delta_url,
        }

    # ------------------------------------------------------------------ init
    def init_repos(self):
        """Ensure both seed and delta repos are initialised."""
        self._init_repo(self.seed_dir)
        self._init_repo(self.delta_dir)

    def _init_repo(self, repo_dir: Path):
        repo_dir.mkdir(parents=True, exist_ok=True)
        git_dir = repo_dir / ".git"
        if not git_dir.exists():
            _run_git(["init"], cwd=repo_dir)
            # Initial empty commit so branches work
            _run_git(["commit", "--allow-empty", "-m", "init"], cwd=repo_dir)
            logger.info(f"Initialised git repo: {repo_dir}")
        else:
            logger.debug(f"Repo already exists: {repo_dir}")

    # --------------------------------------------------------------- staging
    def stage_table(
        self,
        source_dir: Path,
        table_name: str,
        sync_mode: str = "full",
    ) -> str:
        """
        Copy an export folder into the appropriate repo (seed or delta).

        Args:
            source_dir: Path to the export folder (e.g. ``exports/<folder_id>``).
            table_name: Logical table name (used for the subfolder inside the repo).
            sync_mode: ``full`` routes to seed, everything else to delta.

        Returns:
            The repo type the table was staged into ('seed' or 'delta').
        """
        source_dir = Path(source_dir)
        if not source_dir.is_dir():
            raise FileNotFoundError(f"Export directory not found: {source_dir}")

        repo_type = "seed" if sync_mode == "full" else "delta"
        repo_dir = self.seed_dir if repo_type == "seed" else self.delta_dir

        # Ensure repo is initialised
        self._init_repo(repo_dir)

        dest = repo_dir / source_dir.name
        if dest.exists():
            shutil.rmtree(str(dest))
        shutil.copytree(str(source_dir), str(dest))

        _run_git(["add", source_dir.name], cwd=repo_dir)
        logger.info(f"Staged {table_name} ({source_dir.name}) into {repo_type} repo")
        return repo_type

    # -------------------------------------------------------------- committing
    def commit_seed(self, message: str) -> Optional[str]:
        """Commit staged changes in the seed repo."""
        return self._commit(self.seed_dir, message)

    def commit_delta(self, message: str, orphan_branch: Optional[str] = None) -> Optional[str]:
        """
        Commit staged changes in the delta repo.

        If *orphan_branch* is given, the commit is placed on a new orphan
        branch (no parent history), keeping the repo lightweight.
        """
        if orphan_branch:
            self._create_orphan_branch(self.delta_dir, orphan_branch)
        return self._commit(self.delta_dir, message)

    def _repo_dir(self, repo_type: str) -> Path:
        return self.seed_dir if repo_type == "seed" else self.delta_dir

    def _commit(self, repo_dir: Path, message: str) -> Optional[str]:
        result = _run_git(["status", "--porcelain"], cwd=repo_dir)
        if not result.stdout.strip():
            logger.info("Nothing to commit")
            return None
        _run_git(["add", "."], cwd=repo_dir)
        _run_git(["commit", "-m", message], cwd=repo_dir)
        sha = _run_git(["rev-parse", "HEAD"], cwd=repo_dir).stdout.strip()
        logger.info(f"Committed {sha[:8]} to {repo_dir.name}: {message}")
        return sha

    def squash_history(self, repo_type: str, message: str) -> Optional[str]:
        """
        Collapse all history into a single commit.

        Encrypted Parquet files are random bytes that Git cannot
        delta-compress, so keeping history only wastes space.  This
        replaces the entire branch with one commit containing the
        current tree, then runs ``gc`` to discard old objects.
        """
        repo_dir = self._repo_dir(repo_type)

        _run_git(["add", "."], cwd=repo_dir)

        result = _run_git(["status", "--porcelain"], cwd=repo_dir)
        tree_empty = not any(
            (repo_dir / p).exists()
            for p in repo_dir.iterdir()
            if p.name != ".git"
        )
        if tree_empty:
            logger.info("Nothing to squash")
            return None

        # Create a new orphan branch, add everything, commit, then
        # rename it back to the original branch.
        branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_dir).stdout.strip()

        _run_git(["checkout", "--orphan", "_squash_tmp"], cwd=repo_dir)
        _run_git(["add", "."], cwd=repo_dir)
        _run_git(["commit", "-m", message], cwd=repo_dir)

        # Replace the original branch
        _run_git(["branch", "-D", branch], cwd=repo_dir, check=False)
        _run_git(["branch", "-m", branch], cwd=repo_dir)

        # Discard unreachable objects
        _run_git(["reflog", "expire", "--expire=now", "--all"], cwd=repo_dir, check=False)
        _run_git(["gc", "--prune=now"], cwd=repo_dir, check=False)

        sha = _run_git(["rev-parse", "HEAD"], cwd=repo_dir).stdout.strip()
        logger.info(f"Squashed {repo_type} repo to single commit {sha[:8]}")
        return sha

    # --------------------------------------------------------- remote push/pull
    def set_remote(self, repo_type: str, url: str, remote_name: str = "origin"):
        """Configure the remote URL for a repo (idempotent)."""
        repo_dir = self._repo_dir(repo_type)
        self._urls[repo_type] = url

        result = _run_git(["remote"], cwd=repo_dir)
        remotes = result.stdout.strip().splitlines()
        if remote_name in remotes:
            _run_git(["remote", "set-url", remote_name, url], cwd=repo_dir)
        else:
            _run_git(["remote", "add", remote_name, url], cwd=repo_dir)
        logger.info(f"Set {repo_type} remote ({remote_name}) -> {url}")

    def push(self, repo_type: str, remote_name: str = "origin", force: bool = True) -> Dict[str, Any]:
        """
        Push the repo to its configured remote.

        Defaults to ``--force`` because the pipeline squashes history
        before each push — the remote should always match the single
        local commit exactly.
        """
        repo_dir = self._repo_dir(repo_type)
        url = self._urls.get(repo_type)

        if url:
            self.set_remote(repo_type, url, remote_name)

        branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_dir).stdout.strip()

        args = ["push", "-u", remote_name, branch]
        if force:
            args.insert(1, "--force")

        _run_git(args, cwd=repo_dir)

        sha = _run_git(["rev-parse", "--short", "HEAD"], cwd=repo_dir).stdout.strip()
        logger.info(f"Pushed {repo_type} repo ({branch} @ {sha}) to {remote_name}")

        return {
            "repo_type": repo_type,
            "branch": branch,
            "head": sha,
            "remote": remote_name,
        }

    def pull(self, repo_type: str, remote_name: str = "origin") -> Dict[str, Any]:
        """
        Pull the latest data from the remote.

        If the local repo does not exist yet, clones with ``--depth 1``
        (only the latest commit — no history).  Otherwise fetches the
        latest commit and hard-resets to it so the local copy is an
        exact mirror of the remote without accumulating history.
        """
        repo_dir = self._repo_dir(repo_type)
        url = self._urls.get(repo_type)

        git_dir = repo_dir / ".git"

        if not git_dir.exists():
            if not url:
                raise ValueError(
                    f"No remote URL configured for {repo_type} repo and local "
                    f"repo does not exist. Set {repo_type.upper()}_REPO_URL in .env"
                )
            return self.clone_from_remote(url, repo_dir)

        if url:
            self.set_remote(repo_type, url, remote_name)

        # Fetch only the latest commit (--depth 1 keeps the local
        # repo small even if the remote has history).
        _run_git(["fetch", "--depth", "1", remote_name], cwd=repo_dir, check=False)

        branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_dir).stdout.strip()
        _run_git(["reset", "--hard", f"{remote_name}/{branch}"], cwd=repo_dir, check=False)

        sha = _run_git(["rev-parse", "--short", "HEAD"], cwd=repo_dir).stdout.strip()
        logger.info(f"Pulled {repo_type} repo: {branch} @ {sha}")

        return {
            "repo_type": repo_type,
            "target_dir": str(repo_dir),
            "branch": branch,
            "head": sha,
        }

    @staticmethod
    def clone_from_remote(url: str, target_dir: Path) -> Dict[str, Any]:
        """Clone only the latest commit from a remote repo (``--depth 1``)."""
        target_dir = Path(target_dir)
        target_dir.parent.mkdir(parents=True, exist_ok=True)

        _run_git(["clone", "--depth", "1", url, str(target_dir.resolve())], cwd=target_dir.parent)

        sha = _run_git(["rev-parse", "--short", "HEAD"], cwd=target_dir).stdout.strip()
        branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=target_dir).stdout.strip()
        logger.info(f"Cloned {url} -> {target_dir} ({branch} @ {sha})")

        return {
            "target_dir": str(target_dir),
            "branch": branch,
            "head": sha,
        }

    # -------------------------------------------------------- orphan branches
    def _create_orphan_branch(self, repo_dir: Path, branch_name: str):
        _run_git(["checkout", "--orphan", branch_name], cwd=repo_dir)
        # Remove index so only newly-added files appear
        _run_git(["rm", "-rf", "--cached", "."], cwd=repo_dir, check=False)
        logger.info(f"Created orphan branch: {branch_name}")

    def create_delta_orphan(self, run_label: Optional[str] = None) -> str:
        """
        Create a timestamped orphan branch in the delta repo.

        Returns:
            Branch name that was created.
        """
        label = run_label or datetime.utcnow().strftime("run_%Y-%m-%d_%H%M%S")
        self._create_orphan_branch(self.delta_dir, label)
        return label

    def cleanup_old_branches(self, repo_type: str = "delta", keep_latest: int = 5):
        """
        Delete old branches, keeping only the *keep_latest* most recent.

        Reduces repo size on the producer side after bundles have been sent.
        """
        repo_dir = self.delta_dir if repo_type == "delta" else self.seed_dir
        result = _run_git(
            ["branch", "--sort=-committerdate", "--format=%(refname:short)"],
            cwd=repo_dir,
        )
        branches = [b.strip() for b in result.stdout.strip().splitlines() if b.strip()]
        current = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_dir).stdout.strip()

        to_delete = [b for b in branches if b != current][keep_latest:]
        for branch in to_delete:
            _run_git(["branch", "-D", branch], cwd=repo_dir, check=False)
            logger.info(f"Deleted branch: {branch}")
        return to_delete

    # ---------------------------------------------------------------- bundles
    def create_bundle(
        self,
        repo_type: str,
        output_path: Path,
        ref: str = "HEAD",
    ) -> Dict[str, Any]:
        """
        Create a ``git bundle`` for offline / air-gapped transfer.

        Args:
            repo_type: 'seed' or 'delta'.
            output_path: Where to write the ``.bundle`` file.
            ref: Git ref to bundle (default HEAD — the current branch tip).

        Returns:
            Dict with bundle path and size.
        """
        repo_dir = self.seed_dir if repo_type == "seed" else self.delta_dir
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        _run_git(["bundle", "create", str(output_path.resolve()), ref], cwd=repo_dir)

        size = output_path.stat().st_size
        logger.info(f"Created bundle: {output_path}  ({size / (1024*1024):.2f} MB)")

        return {
            "bundle_path": str(output_path),
            "size_bytes": size,
            "size_mb": size / (1024 * 1024),
            "ref": ref,
        }

    def create_incremental_bundle(
        self,
        repo_type: str,
        output_path: Path,
        since_ref: str = "",
    ) -> Dict[str, Any]:
        """
        Create an incremental bundle containing only objects added since *since_ref*.

        If the consumer already has a previous bundle applied, they only
        need the incremental part.

        Args:
            repo_type: 'seed' or 'delta'.
            output_path: Where to write the bundle.
            since_ref: Commit/tag the consumer already has (e.g. a tag you
                       created after the last bundle).
        """
        repo_dir = self.seed_dir if repo_type == "seed" else self.delta_dir
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        ref_spec = f"{since_ref}..HEAD" if since_ref else "HEAD"
        _run_git(["bundle", "create", str(output_path.resolve()), ref_spec], cwd=repo_dir)

        size = output_path.stat().st_size
        logger.info(f"Created incremental bundle: {output_path}  ({size / (1024*1024):.2f} MB)")
        return {
            "bundle_path": str(output_path),
            "size_bytes": size,
            "size_mb": size / (1024 * 1024),
            "ref": ref_spec,
        }

    @staticmethod
    def apply_bundle(
        bundle_path: Path,
        target_dir: Path,
        remote_name: str = "origin",
    ) -> Dict[str, Any]:
        """
        Apply a ``git bundle`` on the consumer side.

        If *target_dir* is not yet a git repo, it will be cloned from the
        bundle.  Otherwise the bundle is fetched as a remote and merged.

        Args:
            bundle_path: Path to the ``.bundle`` file.
            target_dir: Where to clone / fetch into.
            remote_name: Git remote name to register the bundle under.

        Returns:
            Dict with target dir, branch, and latest SHA.
        """
        bundle_path = Path(bundle_path).resolve()
        target_dir = Path(target_dir)

        if not bundle_path.is_file():
            raise FileNotFoundError(f"Bundle not found: {bundle_path}")

        git_dir = target_dir / ".git"

        if not git_dir.exists():
            # Fresh clone from bundle
            target_dir.mkdir(parents=True, exist_ok=True)
            _run_git(
                ["clone", str(bundle_path), str(target_dir.resolve())],
                cwd=target_dir.parent,
            )
            logger.info(f"Cloned from bundle into {target_dir}")
        else:
            # Fetch into existing repo
            # Check if remote exists
            result = _run_git(["remote"], cwd=target_dir)
            remotes = result.stdout.strip().splitlines()
            if remote_name in remotes:
                _run_git(["remote", "set-url", remote_name, str(bundle_path)], cwd=target_dir)
            else:
                _run_git(["remote", "add", remote_name, str(bundle_path)], cwd=target_dir)

            _run_git(["fetch", remote_name], cwd=target_dir)

            # Determine which branch to merge
            bundle_refs = _run_git(["bundle", "list-heads", str(bundle_path)], cwd=target_dir)
            branches = []
            for line in bundle_refs.stdout.strip().splitlines():
                parts = line.split()
                if len(parts) >= 2:
                    ref = parts[1]
                    if ref.startswith("refs/heads/"):
                        branches.append(ref.replace("refs/heads/", ""))

            if branches:
                target_branch = branches[0]
                current = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=target_dir).stdout.strip()
                if current != target_branch:
                    _run_git(["checkout", target_branch], cwd=target_dir, check=False)
                _run_git(["merge", f"{remote_name}/{target_branch}", "--ff-only"], cwd=target_dir, check=False)

            logger.info(f"Applied bundle to {target_dir}")

        sha = _run_git(["rev-parse", "HEAD"], cwd=target_dir).stdout.strip()
        branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=target_dir).stdout.strip()
        return {
            "target_dir": str(target_dir),
            "branch": branch,
            "head_sha": sha,
        }

    # ------------------------------------------------------------ tag helpers
    def tag(self, repo_type: str, tag_name: str, message: str = ""):
        """Create an annotated tag (useful as a baseline for incremental bundles)."""
        repo_dir = self.seed_dir if repo_type == "seed" else self.delta_dir
        args = ["tag"]
        if message:
            args += ["-a", tag_name, "-m", message]
        else:
            args.append(tag_name)
        _run_git(args, cwd=repo_dir)
        logger.info(f"Tagged {repo_type} repo: {tag_name}")

    # -------------------------------------------------------------- info / status
    def status(self, repo_type: str = "both") -> Dict[str, Any]:
        """Return a summary of repo state (current branch, last commit, size)."""
        info: Dict[str, Any] = {}
        repos = []
        if repo_type in ("seed", "both"):
            repos.append(("seed", self.seed_dir))
        if repo_type in ("delta", "both"):
            repos.append(("delta", self.delta_dir))

        for name, repo_dir in repos:
            if not (repo_dir / ".git").exists():
                info[name] = {"initialised": False}
                continue

            branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_dir).stdout.strip()
            sha = _run_git(["rev-parse", "--short", "HEAD"], cwd=repo_dir).stdout.strip()
            log_line = _run_git(["log", "-1", "--format=%s (%ar)"], cwd=repo_dir).stdout.strip()

            # Count objects for rough size
            count_result = _run_git(["count-objects", "-v"], cwd=repo_dir)
            size_kb = 0
            for line in count_result.stdout.splitlines():
                if line.startswith("size-pack:"):
                    size_kb = int(line.split(":")[1].strip())

            info[name] = {
                "initialised": True,
                "branch": branch,
                "head": sha,
                "last_commit": log_line,
                "pack_size_mb": size_kb / 1024,
            }

        return info

    def get_data_dir(self, table_folder: str, sync_mode: str) -> Optional[Path]:
        """
        Return the path inside the appropriate repo where a table's data lives.

        Useful for import_data.py to resolve import directories.
        """
        repo_dir = self.seed_dir if sync_mode == "full" else self.delta_dir
        candidate = repo_dir / table_folder
        return candidate if candidate.is_dir() else None

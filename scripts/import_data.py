#!/usr/bin/env python3
"""
Data Import Script - Phase 2
Decrypt and load data from encrypted files to PostgreSQL

Supports sync modes (full / incremental / upsert) and resumable imports.

Usage:
    python scripts/import_data.py --table financial_data
    python scripts/import_data.py --all
    python scripts/import_data.py --table financial_data --from-bundle bundles/delta_20260402.bundle
    python scripts/import_data.py --table financial_data --from-archive exports/TABLE.tar.gz
"""
import sys
import json
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from pipeline.transformers.encryptor import FileEncryptor
from pipeline.transformers.obfuscator import DataObfuscator
from pipeline.loaders.data_loader import PostgreSQLDataLoader, ChunkCheckpoint
from pipeline.config.settings import get_settings
from pipeline.utils.logger import get_logger
import yaml
import tempfile

logger = get_logger(__name__)


def _resolve_import_dir(table_name: str, search_dirs) -> tuple:
    """
    Search one or more base directories for a table's export folder.

    Returns (import_dir, obfuscated) or raises FileNotFoundError.
    """
    if isinstance(search_dirs, (str, Path)):
        search_dirs = [search_dirs]

    obfuscator = DataObfuscator()
    folder_id = obfuscator.generate_folder_id(table_name)
    tried = []

    for base in search_dirs:
        base_path = Path(base)

        plain = base_path / table_name
        if plain.exists():
            return plain, False

        obfuscated = base_path / folder_id
        if obfuscated.exists():
            return obfuscated, True

        tried.append(str(plain))
        tried.append(str(obfuscated))

    raise FileNotFoundError(
        f"Import directory not found for table: {table_name}\n"
        + "\n".join(f"   Tried: {t}" for t in tried)
    )


def import_table(
    table_config: dict,
    password: str,
    import_base_dir,
    truncate_first: bool = False,
    keep_decrypted: bool = False,
    resume: bool = True,
):
    table_name = table_config["name"]
    pg_config = table_config["postgres"]
    sync_mode = table_config.get("sync_mode", "full")
    merge_keys = table_config.get("merge_keys", [])

    print("\n" + "=" * 70)
    print(f"IMPORTING TABLE: {table_name}  [sync_mode={sync_mode}]")
    print("=" * 70)

    # Resolve import directory (searches plain and obfuscated names
    # across all provided base directories)
    import_dir, obfuscated = _resolve_import_dir(table_name, import_base_dir)
    if obfuscated:
        print(f"Found obfuscated folder: {import_dir.name}")
    else:
        print(f"Found folder: {import_dir.name}")

    logger.info(f"Import directory: {import_dir}")

    # Read manifest
    manifest = _load_manifest(import_dir, table_name, password, obfuscated)

    print(f"\n  Export date: {manifest['export_timestamp']}")
    print(f"  Total rows: {manifest['total_rows']:,}")
    print(f"  Total chunks: {manifest['total_chunks']}")
    sf_filter = manifest.get("snowflake_source", {}).get("filter")
    print(f"  Filter: {sf_filter or 'None (all data)'}")

    encryptor = FileEncryptor()
    loader = PostgreSQLDataLoader()
    checkpoint = ChunkCheckpoint(state_dir=get_settings().state_dir) if resume else None
    loaded_chunks = checkpoint.get_loaded_chunks(table_name) if checkpoint else set()

    if truncate_first or sync_mode == "full":
        print(f"\n  Truncating {pg_config['schema']}.{pg_config['table']}...")
        loader.truncate_table(pg_config["schema"], pg_config["table"])
        if checkpoint:
            checkpoint.clear(table_name)
            loaded_chunks = set()

    print(f"\n  Processing {manifest['total_chunks']} chunks...")

    total_loaded = 0
    temp_files = []

    for chunk_info in manifest["chunks"]:
        chunk_num = chunk_info["chunk_number"]
        encrypted_file = import_dir / chunk_info["file"]

        if chunk_num in loaded_chunks:
            print(f"\n  Chunk {chunk_num}/{manifest['total_chunks']}: already loaded (resume)")
            total_loaded += chunk_info["rows"]
            continue

        print(f"\n  Chunk {chunk_num}/{manifest['total_chunks']}:")
        print(f"   File: {encrypted_file.name}")
        print(f"   Rows: {chunk_info['rows']:,}")

        if not encrypted_file.exists():
            raise FileNotFoundError(f"Encrypted file not found: {encrypted_file}")

        decrypted_file = import_dir / f"data_chunk_{chunk_num:03d}.parquet"
        print("   Decrypting...")

        try:
            encryptor.decrypt_file(encrypted_file, decrypted_file, password)
        except Exception as e:
            if "authentication" in str(e).lower():
                print("\n   Decryption failed - wrong password or corrupted file")
                raise ValueError("Wrong password or corrupted file")
            raise

        print("   Verifying checksum...")
        if not encryptor.verify_checksum(decrypted_file, chunk_info["checksum_sha256"]):
            raise ValueError(f"Checksum mismatch for chunk {chunk_num}")

        temp_files.append(decrypted_file)

        print("   Loading to PostgreSQL...")
        load_info = loader.load_parquet_to_table(
            decrypted_file,
            pg_config["schema"],
            pg_config["table"],
            sync_mode=sync_mode,
            merge_keys=merge_keys,
        )

        total_loaded += load_info["rows_loaded"]
        method = load_info.get("method", "copy")
        print(f"   Loaded {load_info['rows_loaded']:,} rows [{method}]")

        if checkpoint:
            checkpoint.mark_chunk_loaded(table_name, chunk_num)

    # Verify row count (meaningful for full/truncate loads)
    if sync_mode == "full" or truncate_first:
        print("\n  Verifying row count...")
        if loader.verify_row_count(pg_config["schema"], pg_config["table"], manifest["total_rows"]):
            print(f"  Row count verified: {manifest['total_rows']:,}")
        else:
            print("  Row count mismatch - check logs")

    # Cleanup
    if not keep_decrypted:
        for temp_file in temp_files:
            if temp_file.exists():
                temp_file.unlink()
        print(f"\n  Removed {len(temp_files)} temporary files")

    if checkpoint:
        checkpoint.clear(table_name)

    table_info = loader.get_table_info(pg_config["schema"], pg_config["table"])

    print("\n" + "=" * 70)
    print("IMPORT COMPLETE")
    print("=" * 70)
    print(f"  Total: {total_loaded:,} rows loaded [{sync_mode}]")
    print(f"  Table: {pg_config['schema']}.{pg_config['table']}")
    print(f"  Size: {table_info['table_size']}")
    print("=" * 70)


def _load_manifest(import_dir: Path, table_name: str, password: str, obfuscated: bool) -> dict:
    """Locate and load the export manifest (plain or encrypted)."""
    manifest_file = import_dir / "manifest.json"
    if manifest_file.exists():
        with open(manifest_file, "r") as f:
            return json.load(f)

    if not obfuscated:
        raise FileNotFoundError(f"Manifest file not found: {manifest_file}")

    encryptor = FileEncryptor()
    for enc_file in import_dir.glob("*.enc"):
        if enc_file.stat().st_size > 1024 * 1024:
            continue
        try:
            with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tmp:
                temp_path = Path(tmp.name)
            encryptor.decrypt_file(enc_file, temp_path, password)
            with open(temp_path, "r") as f:
                content = json.load(f)
            temp_path.unlink()
            if content.get("table_name") == table_name:
                return content
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            continue

    raise FileNotFoundError(f"Encrypted manifest not found for table: {table_name}")


def _prepare_from_pull(pull_target: str, settings) -> list:
    """
    Pull from remote seed/delta repos.

    Returns a list of directories to search for table data.
    """
    from pipeline.utils.repo_manager import GitRepoManager

    mgr = GitRepoManager(
        seed_dir=settings.seed_repo_dir,
        delta_dir=settings.delta_repo_dir,
        seed_url=settings.seed_repo_url,
        delta_url=settings.delta_repo_url,
    )

    pulled_dirs = []

    if pull_target in ("seed", "both") and settings.seed_repo_url:
        print(f"\n  Pulling seed repo from {settings.seed_repo_url} ...")
        info = mgr.pull("seed")
        print(f"  Seed ready: {info['branch']} @ {info['head']}")
        pulled_dirs.append(settings.seed_repo_dir)
    elif pull_target in ("seed", "both"):
        print("  Seed: no SEED_REPO_URL configured, skipping")

    if pull_target in ("delta", "both") and settings.delta_repo_url:
        print(f"\n  Pulling delta repo from {settings.delta_repo_url} ...")
        info = mgr.pull("delta")
        print(f"  Delta ready: {info['branch']} @ {info['head']}")
        pulled_dirs.append(settings.delta_repo_dir)
    elif pull_target in ("delta", "both"):
        print("  Delta: no DELTA_REPO_URL configured, skipping")

    if not pulled_dirs:
        raise ValueError(
            "No repos pulled. Set SEED_REPO_URL and/or DELTA_REPO_URL in .env"
        )

    return pulled_dirs


def _prepare_from_bundle(bundle_path: str, settings) -> str:
    """
    Apply a git bundle into the import directory, returning the effective
    import_base_dir to use for the rest of the import.
    """
    from pipeline.utils.repo_manager import GitRepoManager

    bundle = Path(bundle_path)
    target = Path(settings.import_base_dir) / f"_bundle_{bundle.stem}"
    print(f"\n  Applying git bundle: {bundle.name}")
    info = GitRepoManager.apply_bundle(bundle, target)
    print(f"  Extracted to {info['target_dir']}  (branch: {info['branch']})")
    return str(target)


def _prepare_from_archive(archive_path: str, settings) -> str:
    """
    Extract a tar archive into the import directory, returning the effective
    import_base_dir.
    """
    from pipeline.utils.archive import ExportArchiver

    archive = Path(archive_path)
    output_dir = Path(settings.import_base_dir)
    print(f"\n  Extracting archive: {archive.name}")
    archiver = ExportArchiver()
    info = archiver.extract_archive(archive, output_dir)
    print(f"  Extracted {info['file_count']} files to {info['output_dir']}")
    return str(output_dir)


def main():
    parser = argparse.ArgumentParser(description="Import data from encrypted files to PostgreSQL")
    parser.add_argument("--table", help="Table name to import")
    parser.add_argument("--all", action="store_true", help="Import all configured tables")
    parser.add_argument("--truncate", action="store_true", help="Truncate table before loading")
    parser.add_argument("--keep-decrypted", action="store_true", help="Keep decrypted Parquet files")
    parser.add_argument("--no-resume", action="store_true", help="Disable chunk-level resume")
    # --- Delivery source flags ---
    parser.add_argument(
        "--pull",
        nargs="?",
        const="both",
        choices=["seed", "delta", "both"],
        metavar="{seed,delta,both}",
        help="Pull latest data from remote Git repos before importing (requires SEED_REPO_URL / DELTA_REPO_URL in .env). Default: both"
    )
    parser.add_argument(
        "--from-bundle",
        metavar="PATH",
        help="Import from a git bundle file (applies bundle into import dir first)"
    )
    parser.add_argument(
        "--from-archive",
        metavar="PATH",
        help="Import from a tar.gz archive (extracts into import dir first)"
    )

    args = parser.parse_args()

    if not args.table and not args.all:
        print("Error: Must specify either --table <name> or --all")
        sys.exit(1)

    try:
        settings = get_settings()
        import_base_dir = settings.import_base_dir
        password = settings.encryption_password

        # Pre-process delivery source — --pull returns a list of dirs,
        # the other modes return a single dir string (wrapped in a list for
        # uniform handling by _resolve_import_dir).
        if args.pull:
            import_base_dir = _prepare_from_pull(args.pull, settings)
        elif args.from_bundle:
            import_base_dir = _prepare_from_bundle(args.from_bundle, settings)
        elif args.from_archive:
            import_base_dir = _prepare_from_archive(args.from_archive, settings)

        with open("config/tables.yaml", "r") as f:
            config = yaml.safe_load(f)

        if args.table:
            table_config = next((t for t in config["tables"] if t["name"] == args.table), None)
            if not table_config:
                print(f"Error: Table '{args.table}' not found in config/tables.yaml")
                sys.exit(1)
            import_table(
                table_config,
                password,
                import_base_dir,
                truncate_first=args.truncate,
                keep_decrypted=args.keep_decrypted,
                resume=not args.no_resume,
            )
        else:
            print(f"\n{'=' * 70}")
            print(f"IMPORTING {len(config['tables'])} TABLES")
            print(f"{'=' * 70}")
            for table_config in config["tables"]:
                try:
                    import_table(
                        table_config,
                        password,
                        import_base_dir,
                        truncate_first=args.truncate,
                        keep_decrypted=args.keep_decrypted,
                        resume=not args.no_resume,
                    )
                except Exception as e:
                    logger.error(f"Failed to import {table_config['name']}: {e}")
                    print(f"\n  Failed to import {table_config['name']}: {e}")
            print(f"\n{'=' * 70}")
            print("ALL IMPORTS COMPLETE")
            print(f"{'=' * 70}")

    except KeyboardInterrupt:
        print("\n\nImport cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Import failed: {e}")
        print(f"\n  Import failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

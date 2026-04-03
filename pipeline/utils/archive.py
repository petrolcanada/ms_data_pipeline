"""
Export Archiver
Bundles export directories into single-file archives for transport.

Encrypted Parquet chunks are already compressed, so the tar wrapper is
primarily for convenience (one file to transfer) rather than additional
compression.  Gzip is applied at level 1 (fast) by default since the
payload is essentially random bytes after AES-GCM encryption.
"""
import tarfile
import io
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class ExportArchiver:
    """Pack and unpack export directories into transportable archives."""

    def __init__(self, compression: str = "gz", compression_level: int = 1):
        """
        Args:
            compression: Archive compression — 'gz', 'bz2', 'xz', or '' for none.
            compression_level: Compresslevel passed to gzip/bz2 (1-9).
        """
        self.compression = compression
        self.compression_level = compression_level

    @property
    def _extension(self) -> str:
        ext_map = {"gz": ".tar.gz", "bz2": ".tar.bz2", "xz": ".tar.xz", "": ".tar"}
        return ext_map.get(self.compression, ".tar.gz")

    def create_archive(
        self,
        source_dir: Path,
        output_path: Optional[Path] = None,
        label: str = "",
    ) -> Dict[str, Any]:
        """
        Archive an export directory into a single file.

        Args:
            source_dir: Directory containing encrypted chunks + manifest.
            output_path: Where to write the archive.  Defaults to
                         ``source_dir.parent / <source_dir.name><ext>``.
            label: Optional human label stored in archive metadata.

        Returns:
            Dict with archive path, file count, and total size.
        """
        source_dir = Path(source_dir)
        if not source_dir.is_dir():
            raise FileNotFoundError(f"Source directory not found: {source_dir}")

        if output_path is None:
            output_path = source_dir.parent / f"{source_dir.name}{self._extension}"
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        mode = f"w:{self.compression}" if self.compression else "w"
        open_kwargs = {}
        if self.compression == "gz":
            open_kwargs["compresslevel"] = self.compression_level

        file_count = 0
        with tarfile.open(str(output_path), mode, **open_kwargs) as tar:
            for item in sorted(source_dir.rglob("*")):
                if item.is_file():
                    arcname = item.relative_to(source_dir.parent).as_posix()
                    tar.add(str(item), arcname=arcname)
                    file_count += 1

            # Embed a small metadata record so the consumer can inspect
            # the archive without extracting everything.
            meta = {
                "source": source_dir.name,
                "label": label,
                "created_at": datetime.utcnow().isoformat() + "Z",
                "file_count": file_count,
            }
            import json
            meta_bytes = json.dumps(meta, indent=2).encode("utf-8")
            info = tarfile.TarInfo(name=f"{source_dir.name}/.archive_meta.json")
            info.size = len(meta_bytes)
            tar.addfile(info, io.BytesIO(meta_bytes))

        archive_size = output_path.stat().st_size
        logger.info(
            f"Created archive {output_path.name}: "
            f"{file_count} files, {archive_size / (1024*1024):.2f} MB"
        )

        return {
            "archive_path": str(output_path),
            "file_count": file_count,
            "size_bytes": archive_size,
            "size_mb": archive_size / (1024 * 1024),
        }

    def extract_archive(
        self,
        archive_path: Path,
        output_dir: Optional[Path] = None,
    ) -> Dict[str, Any]:
        """
        Extract an archive into *output_dir*.

        Returns:
            Dict with extraction path and file count.
        """
        archive_path = Path(archive_path)
        if not archive_path.is_file():
            raise FileNotFoundError(f"Archive not found: {archive_path}")

        if output_dir is None:
            output_dir = archive_path.parent
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        with tarfile.open(str(archive_path), "r:*") as tar:
            members = tar.getmembers()
            # Security: reject absolute paths and path traversal
            for m in members:
                if m.name.startswith("/") or ".." in m.name:
                    raise ValueError(f"Unsafe path in archive: {m.name}")
            tar.extractall(str(output_dir), members=members)

        file_count = sum(1 for m in members if m.isfile())
        logger.info(f"Extracted {file_count} files to {output_dir}")

        return {
            "output_dir": str(output_dir),
            "file_count": file_count,
        }

    def list_archive(self, archive_path: Path) -> List[Dict[str, Any]]:
        """List contents of an archive without extracting."""
        archive_path = Path(archive_path)
        entries = []
        with tarfile.open(str(archive_path), "r:*") as tar:
            for m in tar.getmembers():
                entries.append({
                    "name": m.name,
                    "size_bytes": m.size,
                    "is_file": m.isfile(),
                })
        return entries


def create_table_archive(
    export_base_dir: Path,
    table_folder: str,
    output_dir: Optional[Path] = None,
    label: str = "",
) -> Dict[str, Any]:
    """
    Convenience: archive a single table's export folder.

    Args:
        export_base_dir: Base export directory (e.g. ``exports/``).
        table_folder: Folder name under export_base_dir (plain or obfuscated).
        output_dir: Where to put the archive file.  Defaults to export_base_dir.
        label: Optional label embedded in archive metadata.
    """
    source = Path(export_base_dir) / table_folder
    if output_dir is None:
        output_dir = Path(export_base_dir)
    output_path = Path(output_dir) / f"{table_folder}.tar.gz"

    archiver = ExportArchiver(compression="gz", compression_level=1)
    return archiver.create_archive(source, output_path, label=label)


def extract_table_archive(
    archive_path: Path,
    output_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Convenience: extract a table archive."""
    archiver = ExportArchiver()
    return archiver.extract_archive(archive_path, output_dir)

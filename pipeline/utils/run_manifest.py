"""
Run Manifest
Writes a JSON summary after each export or import run for auditing.
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class RunManifest:
    """Collect per-table results during a pipeline run and persist a summary."""

    def __init__(self, run_type: str, output_dir: str = "state"):
        self.run_type = run_type  # "export" or "import"
        self.started_at = datetime.now().astimezone().isoformat()
        self.tables: List[Dict[str, Any]] = []
        self.errors: List[Dict[str, Any]] = []
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def record_table(
        self,
        table_name: str,
        rows: int,
        duration_seconds: float,
        sync_mode: str = "full",
        watermark: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ):
        entry: Dict[str, Any] = {
            "table_name": table_name,
            "rows": rows,
            "duration_seconds": round(duration_seconds, 2),
            "sync_mode": sync_mode,
        }
        if watermark:
            entry["watermark_advanced_to"] = watermark
        if extra:
            entry.update(extra)
        self.tables.append(entry)

    def record_error(self, table_name: str, error: str):
        self.errors.append({"table_name": table_name, "error": error})

    def save(self) -> Path:
        manifest = {
            "run_type": self.run_type,
            "started_at": self.started_at,
            "completed_at": datetime.now().astimezone().isoformat(),
            "tables_processed": len(self.tables),
            "tables_failed": len(self.errors),
            "total_rows": sum(t["rows"] for t in self.tables),
            "tables": self.tables,
            "errors": self.errors,
        }

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = self.output_dir / f"run_{self.run_type}_{ts}.json"
        with open(out_path, "w") as f:
            json.dump(manifest, f, indent=2)

        logger.info(f"Run manifest saved to {out_path}")
        return out_path

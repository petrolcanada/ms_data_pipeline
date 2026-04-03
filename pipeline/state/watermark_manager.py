"""
Watermark Manager
Tracks extraction watermarks for incremental data sync
"""
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class WatermarkManager:
    """Manages watermark state files for incremental data extraction.
    
    Each table gets a JSON state file under state_dir tracking the last
    successfully exported watermark value plus a rolling history.
    """

    def __init__(self, state_dir: str = "state"):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _state_file(self, table_name: str) -> Path:
        return self.state_dir / f"{table_name}_watermark.json"

    def get_watermark(self, table_name: str) -> Optional[str]:
        """Return the last exported watermark value, or None for first run."""
        state_file = self._state_file(table_name)
        if not state_file.exists():
            logger.info(f"No watermark found for {table_name} - will perform full extraction")
            return None

        try:
            with open(state_file, "r") as f:
                state = json.load(f)
            watermark = state.get("watermark_value")
            logger.info(f"Loaded watermark for {table_name}: {watermark}")
            return watermark
        except Exception as e:
            logger.error(f"Failed to read watermark for {table_name}: {e}")
            return None

    def update_watermark(
        self,
        table_name: str,
        watermark_value: str,
        rows_exported: int = 0,
        export_timestamp: Optional[str] = None,
    ):
        """Persist a new watermark after a successful export."""
        state: Dict[str, Any] = {
            "table_name": table_name,
            "watermark_value": watermark_value,
            "rows_exported": rows_exported,
            "updated_at": export_timestamp or datetime.utcnow().isoformat() + "Z",
            "history": [],
        }

        state_file = self._state_file(table_name)
        if state_file.exists():
            try:
                with open(state_file, "r") as f:
                    old_state = json.load(f)
                history = old_state.get("history", [])
                history.append(
                    {
                        "watermark_value": old_state.get("watermark_value"),
                        "rows_exported": old_state.get("rows_exported"),
                        "updated_at": old_state.get("updated_at"),
                    }
                )
                state["history"] = history[-10:]
            except Exception:
                pass

        with open(state_file, "w") as f:
            json.dump(state, f, indent=2)

        logger.info(f"Updated watermark for {table_name}: {watermark_value}")

    def reset_watermark(self, table_name: str):
        """Delete the watermark file to force a full extraction next run."""
        state_file = self._state_file(table_name)
        if state_file.exists():
            state_file.unlink()
            logger.info(f"Reset watermark for {table_name}")

    def get_all_watermarks(self) -> Dict[str, Any]:
        """Return a dict of all persisted watermarks keyed by table name."""
        watermarks: Dict[str, Any] = {}
        for state_file in self.state_dir.glob("*_watermark.json"):
            table_name = state_file.stem.replace("_watermark", "")
            try:
                with open(state_file, "r") as f:
                    watermarks[table_name] = json.load(f)
            except Exception as e:
                logger.error(f"Failed to read watermark for {table_name}: {e}")
        return watermarks

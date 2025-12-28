"""
Metadata Comparator
Compares table metadata to detect schema changes
"""
from typing import Dict, List, Any
from pipeline.utils.logger import get_logger

logger = get_logger(__name__)


class MetadataComparator:
    """
    Compare table metadata to detect changes
    
    Detects:
    - Column additions/removals
    - Column type changes
    - Column order changes
    - Constraint changes
    - Table property changes
    """
    
    def __init__(self):
        self.changes = []
    
    def compare_metadata(self, old_metadata: Dict, new_metadata: Dict) -> Dict:
        """
        Compare two metadata dictionaries
        
        Args:
            old_metadata: Previous metadata
            new_metadata: Current metadata
            
        Returns:
            {
                "changed": True/False,
                "changes": [
                    {"type": "column_added", "column": "NEW_COL", "details": {...}},
                    ...
                ],
                "summary": "2 columns added, 1 type changed"
            }
        """
        self.changes = []
        
        # Compare columns
        self._compare_columns(
            old_metadata.get('columns', []),
            new_metadata.get('columns', [])
        )
        
        # Compare constraints
        self._compare_constraints(
            old_metadata.get('constraints', {}),
            new_metadata.get('constraints', {})
        )
        
        # Compare table properties
        self._compare_properties(old_metadata, new_metadata)
        
        # Generate summary
        summary = self._generate_summary()
        
        return {
            "changed": len(self.changes) > 0,
            "changes": self.changes,
            "summary": summary
        }
    
    def _compare_columns(self, old_columns: List[Dict], new_columns: List[Dict]):
        """Compare column definitions"""
        # Create lookup dictionaries
        old_cols_dict = {col['name']: col for col in old_columns}
        new_cols_dict = {col['name']: col for col in new_columns}
        
        old_col_names = set(old_cols_dict.keys())
        new_col_names = set(new_cols_dict.keys())
        
        # Detect added columns
        added_cols = new_col_names - old_col_names
        for col_name in added_cols:
            col = new_cols_dict[col_name]
            self.changes.append({
                "type": "column_added",
                "column": col_name,
                "details": {
                    "data_type": col.get('data_type'),
                    "nullable": col.get('nullable'),
                    "position": col.get('ordinal_position')
                }
            })
            logger.info(f"Column added: {col_name} ({col.get('data_type')})")
        
        # Detect removed columns
        removed_cols = old_col_names - new_col_names
        for col_name in removed_cols:
            col = old_cols_dict[col_name]
            self.changes.append({
                "type": "column_removed",
                "column": col_name,
                "details": {
                    "data_type": col.get('data_type')
                }
            })
            logger.info(f"Column removed: {col_name}")
        
        # Detect modified columns
        common_cols = old_col_names & new_col_names
        for col_name in common_cols:
            old_col = old_cols_dict[col_name]
            new_col = new_cols_dict[col_name]
            
            # Check type change
            if old_col.get('data_type') != new_col.get('data_type'):
                self.changes.append({
                    "type": "column_type_changed",
                    "column": col_name,
                    "details": {
                        "old_type": old_col.get('data_type'),
                        "new_type": new_col.get('data_type')
                    }
                })
                logger.info(f"Column type changed: {col_name} ({old_col.get('data_type')} → {new_col.get('data_type')})")
            
            # Check nullable change
            if old_col.get('nullable') != new_col.get('nullable'):
                self.changes.append({
                    "type": "column_nullable_changed",
                    "column": col_name,
                    "details": {
                        "old_nullable": old_col.get('nullable'),
                        "new_nullable": new_col.get('nullable')
                    }
                })
                logger.info(f"Column nullable changed: {col_name}")
            
            # Check position change
            if old_col.get('ordinal_position') != new_col.get('ordinal_position'):
                self.changes.append({
                    "type": "column_position_changed",
                    "column": col_name,
                    "details": {
                        "old_position": old_col.get('ordinal_position'),
                        "new_position": new_col.get('ordinal_position')
                    }
                })
                logger.debug(f"Column position changed: {col_name}")
    
    def _compare_constraints(self, old_constraints: Dict, new_constraints: Dict):
        """Compare table constraints"""
        # Compare primary keys
        old_pk = set(old_constraints.get('primary_key', []))
        new_pk = set(new_constraints.get('primary_key', []))
        
        if old_pk != new_pk:
            self.changes.append({
                "type": "primary_key_changed",
                "details": {
                    "old_pk": list(old_pk),
                    "new_pk": list(new_pk)
                }
            })
            logger.info(f"Primary key changed: {old_pk} → {new_pk}")
        
        # Compare foreign keys
        old_fks = old_constraints.get('foreign_keys', [])
        new_fks = new_constraints.get('foreign_keys', [])
        
        if old_fks != new_fks:
            self.changes.append({
                "type": "foreign_keys_changed",
                "details": {
                    "old_count": len(old_fks),
                    "new_count": len(new_fks)
                }
            })
            logger.info(f"Foreign keys changed: {len(old_fks)} → {len(new_fks)}")
    
    def _compare_properties(self, old_metadata: Dict, new_metadata: Dict):
        """Compare table properties"""
        # Compare comment
        old_comment = old_metadata.get('comment', '')
        new_comment = new_metadata.get('comment', '')
        
        if old_comment != new_comment:
            self.changes.append({
                "type": "comment_changed",
                "details": {
                    "old_comment": old_comment,
                    "new_comment": new_comment
                }
            })
            logger.info("Table comment changed")
        
        # Compare clustering key
        old_clustering = old_metadata.get('clustering_key', [])
        new_clustering = new_metadata.get('clustering_key', [])
        
        if old_clustering != new_clustering:
            self.changes.append({
                "type": "clustering_key_changed",
                "details": {
                    "old_clustering": old_clustering,
                    "new_clustering": new_clustering
                }
            })
            logger.info("Clustering key changed")
    
    def _generate_summary(self) -> str:
        """Generate human-readable summary of changes"""
        if not self.changes:
            return "No changes detected"
        
        # Count change types
        type_counts = {}
        for change in self.changes:
            change_type = change['type']
            type_counts[change_type] = type_counts.get(change_type, 0) + 1
        
        # Format summary
        summary_parts = []
        
        if 'column_added' in type_counts:
            count = type_counts['column_added']
            summary_parts.append(f"{count} column{'s' if count > 1 else ''} added")
        
        if 'column_removed' in type_counts:
            count = type_counts['column_removed']
            summary_parts.append(f"{count} column{'s' if count > 1 else ''} removed")
        
        if 'column_type_changed' in type_counts:
            count = type_counts['column_type_changed']
            summary_parts.append(f"{count} type{'s' if count > 1 else ''} changed")
        
        if 'column_nullable_changed' in type_counts:
            count = type_counts['column_nullable_changed']
            summary_parts.append(f"{count} nullable constraint{'s' if count > 1 else ''} changed")
        
        if 'primary_key_changed' in type_counts:
            summary_parts.append("primary key changed")
        
        if 'foreign_keys_changed' in type_counts:
            summary_parts.append("foreign keys changed")
        
        # Add other changes
        other_types = set(type_counts.keys()) - {
            'column_added', 'column_removed', 'column_type_changed',
            'column_nullable_changed', 'primary_key_changed', 'foreign_keys_changed'
        }
        if other_types:
            summary_parts.append(f"{len(other_types)} other change{'s' if len(other_types) > 1 else ''}")
        
        return ", ".join(summary_parts)
    
    def format_changes(self, changes: List[Dict]) -> str:
        """
        Format changes for display
        
        Args:
            changes: List of change dictionaries
            
        Returns:
            Formatted string for console output
        """
        if not changes:
            return "No changes detected"
        
        lines = []
        
        for change in changes:
            change_type = change['type']
            
            if change_type == 'column_added':
                col = change['column']
                details = change['details']
                data_type = details.get('data_type', 'UNKNOWN')
                nullable = "NULL" if details.get('nullable') else "NOT NULL"
                lines.append(f"  • Column added: {col} ({data_type}, {nullable})")
            
            elif change_type == 'column_removed':
                col = change['column']
                lines.append(f"  • Column removed: {col}")
            
            elif change_type == 'column_type_changed':
                col = change['column']
                details = change['details']
                old_type = details.get('old_type', 'UNKNOWN')
                new_type = details.get('new_type', 'UNKNOWN')
                lines.append(f"  • Column type changed: {col} ({old_type} → {new_type})")
            
            elif change_type == 'column_nullable_changed':
                col = change['column']
                details = change['details']
                old_null = "NULL" if details.get('old_nullable') else "NOT NULL"
                new_null = "NULL" if details.get('new_nullable') else "NOT NULL"
                lines.append(f"  • Column nullable changed: {col} ({old_null} → {new_null})")
            
            elif change_type == 'column_position_changed':
                col = change['column']
                details = change['details']
                old_pos = details.get('old_position')
                new_pos = details.get('new_position')
                lines.append(f"  • Column position changed: {col} (position {old_pos} → {new_pos})")
            
            elif change_type == 'primary_key_changed':
                details = change['details']
                old_pk = ', '.join(details.get('old_pk', []))
                new_pk = ', '.join(details.get('new_pk', []))
                lines.append(f"  • Primary key changed: ({old_pk}) → ({new_pk})")
            
            elif change_type == 'foreign_keys_changed':
                details = change['details']
                old_count = details.get('old_count', 0)
                new_count = details.get('new_count', 0)
                lines.append(f"  • Foreign keys changed: {old_count} → {new_count}")
            
            elif change_type == 'comment_changed':
                lines.append(f"  • Table comment changed")
            
            elif change_type == 'clustering_key_changed':
                lines.append(f"  • Clustering key changed")
            
            else:
                lines.append(f"  • {change_type}: {change.get('column', 'N/A')}")
        
        return "\n".join(lines)

#!/usr/bin/env python3
import os
import shutil
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from pipeline import HealingPipeline  # Local import for your VM; change for GitHub

class AuditEngineV2:
    def __init__(self, workspace_path: str):
        """
        Principal Engineer Note: Workspace management with path validation.
        """
        self.workspace = Path(workspace_path).resolve()
        self.pipeline = HealingPipeline()
        self._ensure_workspace()

    def _ensure_workspace(self):
        """Ensures the target directory exists before operations begin."""
        if not self.workspace.exists():
            self.workspace.mkdir(parents=True, exist_ok=True)

    def audit_and_heal_file(self, relative_path: str, dry_run: bool = True, 
                            force_write: bool = False) -> Dict[str, Any]:
        """
        The Core Orchestrator: Validates, Heals, Backups, and Writes.
        
        Args:
            relative_path: Path relative to workspace.
            dry_run: If True, no files are changed.
            force_write: If True, writes even if only 'partial_heal' is achieved.
        """
        full_path = (self.workspace / relative_path).resolve()
        
        # --- PHASE 1: PRE-FLIGHT VALIDATION ---
        if not full_path.exists():
            return self._file_error(relative_path, "FILE_NOT_FOUND", f"File not found: {full_path}")
        
        if not full_path.is_file():
            return self._file_error(relative_path, "NOT_A_FILE", f"Not a regular file: {full_path}")
        
        if not os.access(full_path, os.R_OK):
            return self._file_error(relative_path, "PERMISSION_DENIED", f"Read access denied: {full_path}")
        
        # Check write access on the file and the directory before processing
        if not dry_run:
            if not os.access(full_path, os.W_OK) or not os.access(full_path.parent, os.W_OK):
                return self._file_error(relative_path, "NO_WRITE_PERMISSION", "Write access denied")

        # --- PHASE 2: PROCESSING ---
        result = self.pipeline.heal_manifest(file_path=full_path)
        
        # Logic: Write if successful, OR if partial and force_write is enabled
        should_write = not dry_run and (result['success'] or (result['partial_heal'] and force_write))
        
        # --- PHASE 3: BACKUP & ATOMIC WRITE ---
        if should_write:
            backup_path = self._create_unique_backup(full_path)
            if backup_path:
                try:
                    shutil.copy2(full_path, backup_path)
                    result["backup_created"] = str(backup_path.relative_to(self.workspace))
                except Exception as e:
                    result["backup_warning"] = f"Backup failed: {str(e)}"
            
            try:
                self._atomic_write(full_path, result['content'])
                result["written"] = True
            except IOError as e:
                result["write_error"] = str(e)
                result["success"] = False  # Downgrade to failure if disk write fails
        
        # Attach Metadata
        result["file_path"] = str(relative_path)
        try:
            result["file_size_bytes"] = full_path.stat().st_size
        except OSError:
            result["file_size_bytes"] = 0
            
        return result

    def _create_unique_backup(self, target_path: Path) -> Optional[Path]:
        """
        Generates a unique backup name: service.kubecuro.backup, 
        service-1.kubecuro.backup, etc.
        """
        # Primary naming convention
        backup_path = target_path.with_suffix('.kubecuro.backup')
        
        # If exists, start incrementing a counter
        if backup_path.exists():
            counter = 1
            while True:
                new_name = f"{target_path.stem}-{counter}.kubecuro.backup"
                backup_path = target_path.with_name(new_name)
                if not backup_path.exists():
                    break
                counter += 1
        
        return backup_path

    def _atomic_write(self, target_path: Path, content: str):
        """
        Atomic Write Pattern: Write to temp -> Rename to target.
        Ensures the file is never partially written or corrupted.
        """
        temp_file = target_path.with_suffix('.kubecuro.tmp')
        try:
            temp_file.write_text(content, encoding='utf-8')
            # os.replace is atomic on Unix/Linux
            os.replace(temp_file, target_path)
        except Exception as e:
            if temp_file.exists():
                temp_file.unlink()  # Cleanup the trash
            raise IOError(f"Atomic write failed: {str(e)}")

    def scan_directory(self, extension: str = ".yaml", dry_run: bool = True, 
                       force_write: bool = False, max_depth: int = 10,
                       progress_callback: Optional[Callable[[int, int], None]] = None) -> List[Dict[str, Any]]:
        """
        Recursively scans the workspace for YAML files with depth protection.
        """
        reports = []
        # Support both .yaml and .YAML
        patterns = [f"*{extension.lower()}", f"*{extension.upper()}"]
        
        # Pre-scan for total count to support progress bars
        all_files = []
        for p in patterns:
            all_files.extend([f for f in self.workspace.rglob(p) if f.is_file() and not f.is_symlink()])
        
        total_files = len(all_files)
        processed = 0

        for file_path in all_files:
            # Depth check
            try:
                depth = len(file_path.relative_to(self.workspace).parts)
                if depth > max_depth:
                    continue
            except ValueError:
                continue

            rel_path = str(file_path.relative_to(self.workspace))
            report = self.audit_and_heal_file(rel_path, dry_run, force_write)
            reports.append(report)
            
            processed += 1
            if progress_callback:
                progress_callback(processed, total_files)

        return reports

    def generate_summary(self, reports: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Intelligent Summary: Calculates success rates and suggests force_write.
        """
        if not reports:
            return self._empty_summary()

        total = len(reports)
        successful = sum(1 for r in reports if r.get('success', False))
        partial = sum(1 for r in reports if r.get('partial_heal', False))
        backups = sum(1 for r in reports if r.get('backup_created'))
        writes = sum(1 for r in reports if r.get('written', False))
        
        error_count = sum(1 for r in reports if r.get('status') in 
                          ['FILE_NOT_FOUND', 'PERMISSION_DENIED', 'NO_WRITE_PERMISSION', 'SCAN_ERROR'])

        # Intelligent Logic: If partial heals are few (<10%), suggest force_write to the user
        recommend_force = (partial > 0) and (partial < (total * 0.10))

        return {
            "total_files": total,
            "success_rate": (successful / total) if total > 0 else 0,
            "successful": successful,
            "partial_heal": partial,
            "failed_logic": total - successful - partial - error_count,
            "system_errors": error_count,
            "backups_created": backups,
            "written_to_disk": writes,
            "recommend_force_write": recommend_force,
            "summary_timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def _file_error(self, relative_path: str, status: str, error: str) -> Dict[str, Any]:
        """Internal helper for standardized error structure."""
        return {
            "file_path": relative_path,
            "status": status,
            "error": error,
            "success": False,
            "partial_heal": False
        }

    def _empty_summary(self) -> Dict[str, Any]:
        return {
            "total_files": 0, "success_rate": 0, "successful": 0, 
            "partial_heal": 0, "system_errors": 0, "backups_created": 0
        }

    def cleanup_backups(self, max_age_hours: int = 24) -> int:
        """Removes old .kubecuro.backup files."""
        count = 0
        cutoff = time.time() - (max_age_hours * 3600)
        for backup in self.workspace.rglob("*.kubecuro.backup"):
            if backup.stat().st_mtime < cutoff:
                backup.unlink()
                count += 1
        return count

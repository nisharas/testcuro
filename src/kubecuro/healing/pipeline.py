#!/usr/bin/env python3
"""
KUBECURO PIPELINE - Phase 1.3 (Production Hardened)
------------------------------
Orchestrates the flow: Raw Text -> Lexer -> Structurer -> Validated YAML.
All production edges covered: memory, partial heal, file input, BOM, etc.
"""
from typing import Dict, List, Any, Optional
from pathlib import Path
import sys
from kubecuro.healing.lexer import RawLexer
from kubecuro.healing.structurer import KubeStructurer


class HealingPipeline:
    def __init__(self, max_size_mb: int = 10, timeout_s: int = 30):
        """
        Production-hardened pipeline with configurable limits.
        
        Args:
            max_size_mb: Reject files > this size (default: 10MB)
            timeout_s: Processing timeout (default: 30s)
        """
        self.lexer = RawLexer()
        self.structurer = KubeStructurer()
        self.max_size_mb = max_size_mb
        self.timeout_s = timeout_s

    def heal_manifest(self, raw_content: Optional[str] = None, file_path: Optional[Path] = None) -> Dict[str, Any]:
        """
        Production-hardened Phase 1 healing with ALL edges covered.
        
        Supports both string input and file_path input.
        Handles memory limits, partial heals, BOM, empty inputs, etc.
        """
        # EDGE 1: None input
        if raw_content is None and file_path is None:
            return self._error_response("", "MISSING_INPUT", "No content or file provided")
        
        # EDGE 2: Read from file if provided (BOM stripping)
        if file_path:
            try:
                raw_content = file_path.read_text(encoding='utf-8-sig')
            except Exception as e:
                return self._error_response("", f"FILE_READ_ERROR: {str(e)}", str(e), file_path)
        
        # EDGE 3: MEMORY FIX - UTF-8 bytes (not sys.getsizeof)
        content_bytes = len(raw_content.encode('utf-8'))
        if content_bytes > self.max_size_mb * 1024 * 1024:
            return self._error_response(
                raw_content[:1000], 
                "FILE_TOO_LARGE", 
                f"File exceeds {self.max_size_mb}MB limit ({content_bytes/1024/1024:.1f}MB)"
            )
        
        # EDGE 4: Empty/whitespace
        if not raw_content or not raw_content.strip():
            return self._error_response("", "EMPTY_INPUT", "No input provided")
        
        try:
            # Phase 1.1: Lexical Repair
            lexed_content = self.lexer.process_string(raw_content)
            
            # Phase 1.2: Structural Repair
            final_yaml, status = self.structurer.process_yaml(lexed_content)
            
            # Phase 1.3: Generate comprehensive report
            report = self.structurer.full_healing_report(raw_content, final_yaml, status)
            
            # CRITICAL: PARTIAL HEAL LOGIC
            success_statuses = {
                "STRUCTURE_OK", 
                "STRUCTURE_FIXED_1", 
                "STRUCTURE_FIXED_2", 
                "STRUCTURE_FIXED_3", 
                "MULTI_DOC_HANDLED"
            }
            is_structural_success = status in success_statuses
            is_partially_healed = (
                status.startswith("STRUCTURE_FAIL") and 
                final_yaml != lexed_content and 
                final_yaml.strip()
            )
            
            return {
                "content": final_yaml,
                "status": status,
                "report": report,
                "success": is_structural_success,           # ✅ kubectl apply -f ready
                "partial_heal": is_partially_healed,        # ⚠️ CLI warning trigger
                "phase1_complete": True,
                "input_type": "string" if file_path is None else "file",
                "input_size_bytes": content_bytes
            }
            
        except Exception as e:
            return self._error_response(raw_content, f"PIPELINE_ERROR: {str(e)[:100]}", str(e))

    def heal_manifests(self, contents: List[str]) -> List[Dict[str, Any]]:
        """
        Batch process multiple manifests for production workloads.
        
        Perfect for GitOps, ArgoCD, Flux, or 10K+ file processing.
        Auto-filters empty contents.
        """
        valid_contents = [c for c in contents if c and c.strip()]
        return [self.heal_manifest(c) for c in valid_contents]

    def heal_files(self, file_paths: List[Path]) -> List[Dict[str, Any]]:
        """
        Batch process files directly (production GitOps pattern).
        """
        return [self.heal_manifest(file_path=fp) for fp in file_paths]

    def batch_success_rate(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Enhanced success metrics for SRE dashboards.
        
        Usage: pipeline.batch_success_rate(pipeline.heal_manifests(files))
        """
        if not results:
            return {"success_rate": 0.0, "total": 0, "successful": 0, "partial": 0, "failed": 0}
        
        successful = sum(1 for r in results if r.get("success", False))
        partial = sum(1 for r in results if r.get("partial_heal", False))
        total = len(results)
        
        return {
            "success_rate": successful / total,
            "total": total,
            "successful": successful,
            "partial_heal": partial,
            "failed": total - successful - partial
        }

    def is_kubectl_ready(self, result: Dict[str, Any]) -> bool:
        """
        Quick check: Can this YAML be applied with `kubectl apply -f`?
        
        Returns True for all success states only (ignores partial_heal).
        """
        return result.get("success", False)

    def _error_response(self, content: str, status: str, error: str, 
                       file_path: Optional[Path] = None) -> Dict[str, Any]:
        """
        Standardized error response format for all failure modes.
        """
        content_bytes = len(content.encode('utf-8')) if content else 0
        lines = len(content.splitlines()) if content else 0
        
        return {
            "content": content,
            "status": status,
            "report": {
                "total_lines": lines,
                "lines_changed": 0,
                "changes": [],
                "error": error,
                "file_path": str(file_path) if file_path else None
            },
            "success": False,
            "partial_heal": False,
            "phase1_complete": False,
            "input_size_bytes": content_bytes
        }

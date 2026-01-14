#!/usr/bin/env python3
"""
KUBECURO CLI - The Face of the Project
--------------------------------------
Purpose: Provides a high-performance terminal interface for auditing 
and healing Kubernetes manifests. Includes progress tracking, 
error handling, and actionable summary recommendations.
"""

import sys
import argparse
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn, BarColumn, TaskProgressColumn

# Internal Package Imports
from kubecuro.core.engine import AuditEngineV2

console = Console()

def main():
    parser = argparse.ArgumentParser(
        description="KubeCuro: Enterprise-grade Kubernetes YAML Healing & Auditing",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument("path", help="Directory or file to audit")
    parser.add_argument("--fix", action="store_true", help="Apply fixes to files (requires backup)")
    parser.add_argument("--force", action="store_true", help="Force write even on partial heals")
    parser.add_argument("--ext", default=".yaml", help="File extension to scan")
    parser.add_argument("--depth", type=int, default=10, help="Maximum directory depth")

    args = parser.parse_args()

    # 1. Initialization & Path Validation
    workspace = Path(args.path).resolve()
    if not workspace.exists():
        console.print(Panel(f"[bold red]Error:[/bold red] Path [white]'{workspace}'[/white] does not exist.", border_style="red"))
        sys.exit(1)

    engine = AuditEngineV2(str(workspace))
    
    console.print(Panel.fit(
        "[bold cyan]KubeCuro v0.1.0[/bold cyan]\n"
        f"[dim]Scanning: {workspace}[/dim]\n"
        f"[dim]Mode: {'FIX (with backup)' if args.fix else 'DRY-RUN (read-only)'}[/dim]",
        title="[bold white]Phase 1: Audit & Heal[/bold white]",
        border_style="cyan"
    ))

    # 2. Pre-Scan to handle EDGE 3: Progress Total
    # We find files first so the user sees a meaningful progress bar
    target_files = []
    if workspace.is_file():
        target_files = [workspace]
    else:
        # Shallow scan just to count files for the progress bar total
        target_files = list(workspace.rglob(f"*{args.ext}"))

    # EDGE 2: ZERO FILES DETECTED
    if not target_files:
        console.print(f"\n[bold yellow]⚠️  No YAML files found with extension '{args.ext}'[/bold yellow]")
        console.print(f"[dim]Try running with: --ext .yml  or verify the path: {workspace}[/dim]")
        sys.exit(0)

    # 3. Execution with Enhanced Progress Tracking
    reports = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console
    ) as progress:
        
        task_id = progress.add_task("Processing manifests...", total=len(target_files))
        
        if workspace.is_file():
            # Process single file
            report = engine.audit_and_heal_file(
                workspace.name, 
                dry_run=not args.fix, 
                force_write=args.force
            )
            reports.append(report)
            progress.update(task_id, advance=1)
        else:
            # Process directory with per-file progress updates
            # We use the internal engine scan but track progress here for UI fidelity
            for file_path in target_files:
                # Relative path calculation for the engine
                rel_path = str(file_path.relative_to(workspace))
                report = engine.audit_and_heal_file(
                    rel_path,
                    dry_run=not args.fix,
                    force_write=args.force
                )
                reports.append(report)
                progress.update(task_id, advance=1, description=f"Healed: {file_path.name}")

    # 4. Results Table with EDGE 1: EMPTY REPORTS SAFETY
    table = Table(title="Audit Results", show_lines=True, header_style="bold magenta")
    table.add_column("File Path", style="cyan", no_wrap=False)
    table.add_column("Status", style="bold")
    table.add_column("Success", justify="center")
    table.add_column("Changes", justify="right")

    # Use 'reports or []' to prevent iteration over NoneType
    for r in (reports or []):
        # Determine styling based on state
        is_success = r.get('success', False)
        is_partial = r.get('partial_heal', False)
        
        status_color = "green" if is_success else "yellow" if is_partial else "red"
        success_icon = "✅" if is_success else "⚠️" if is_partial else "❌"
        
        # Double-safe retrieval of lines_changed
        inner_report = r.get('report') or {}
        lines_changed = inner_report.get('lines_changed', 0) or 0
        
        table.add_row(
            str(r.get('file_path', 'unknown')),
            f"[{status_color}]{r.get('status', 'UNKNOWN')}[/{status_color}]",
            success_icon,
            str(lines_changed)
        )

    console.print(table)

    # 5. Final Summary & Contextual Next Steps
    summary = engine.generate_summary(reports)
    
    console.print(f"\n[bold white]Final Summary:[/bold white]")
    console.print(f" • Total Files Scanned: {summary['total_files']}")
    console.print(f" • Success Rate: [bold green]{summary['success_rate']:.1%}[/bold green]")
    console.print(f" • Backups Created: {summary['backups_created']}")
    
    # POLISH: Copy-Paste Command for Partial Heals
    if summary.get('recommend_force_write'):
        console.print("\n[bold yellow]💡 Partial Heals Detected:[/bold yellow]")
        console.print(f"[dim]The parser found errors it couldn't fully map, but best-effort fixes are available.[/dim]")
        console.print(f"[bold cyan]Next:[/bold cyan] kubecuro {args.path} --fix --force")

    # POLISH: Copy-Paste Command for Dry Runs
    if not args.fix:
        console.print(f"\n[dim italic]Note: No changes were made to files. To apply these repairs, run:[/dim italic]")
        console.print(f"[bold cyan]Next:[/bold cyan] kubecuro {args.path} --fix")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Script principal de mantenimiento del proyecto.

Centraliza todas las tareas de mantenimiento:
- Verificación de estructura
- Limpieza de archivos
- Organización automática
- Reportes de estado

Usage:
    python tools/scripts/project_maintenance.py [command] [options]

Commands:
    check     - Check project status (default)
    fix       - Fix found problems
    clean     - Clean temporary files
    full      - Complete maintenance (fix + clean)

Examples:
    # Quick check
    python tools/scripts/project_maintenance.py check
    
    # Corrección automática
    python tools/scripts/project_maintenance.py fix
    
    # Limpieza básica
    python tools/scripts/project_maintenance.py clean
    
    # Mantenimiento completo
    python tools/scripts/project_maintenance.py full --aggressive
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Import our maintenance modules
from tools.scripts.maintain_structure import ProjectStructureMaintainer
from tools.scripts.cleanup_project import ProjectCleaner

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProjectMaintenanceManager:
    """Central manager for all project maintenance tasks."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.maintenance_log = project_root / "logs" / "maintenance.log"
    
    def log_maintenance_run(self, command: str, success: bool, details: str = ""):
        """Log maintenance activities."""
        # Ensure logs directory exists
        self.maintenance_log.parent.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = "SUCCESS" if success else "FAILED"
        
        log_entry = f"[{timestamp}] {command.upper()}: {status}"
        if details:
            log_entry += f" - {details}"
        log_entry += "\n"
        
        with open(self.maintenance_log, "a", encoding="utf-8") as f:
            f.write(log_entry)
    
    def check_project_health(self, verbose: bool = False) -> dict:
        """Perform comprehensive project health check."""
        logger.info("🔍 Running project health check...")
        
        health_report = {
            "structure": {"status": "unknown", "issues": []},
            "cleanliness": {"status": "unknown", "issues": []},
            "overall": "unknown"
        }
        
        # Check structure
        try:
            maintainer = ProjectStructureMaintainer(
                project_root=self.project_root,
                fix_mode=False,
                verbose=verbose
            )
            
            # Check directories
            missing_dirs = maintainer.check_directory_structure()
            misplaced_files = maintainer.find_misplaced_files()
            
            structure_issues = []
            if missing_dirs:
                structure_issues.append(f"{len(missing_dirs)} missing directories")
            
            total_misplaced = sum(len(files) for files in misplaced_files.values())
            if total_misplaced > 0:
                structure_issues.append(f"{total_misplaced} misplaced files")
            
            health_report["structure"]["issues"] = structure_issues
            health_report["structure"]["status"] = "good" if not structure_issues else "needs_attention"
            
        except Exception as e:
            logger.error(f"❌ Structure check failed: {e}")
            health_report["structure"]["status"] = "error"
            health_report["structure"]["issues"] = [str(e)]
        
        # Check cleanliness
        try:
            cleaner = ProjectCleaner(
                project_root=self.project_root,
                aggressive=False,
                dry_run=True
            )
            
            cleanup_targets = cleaner.find_cleanup_targets()
            space_savings = cleaner.calculate_space_savings(cleanup_targets)
            
            cleanliness_issues = []
            if cleanup_targets:
                cleanliness_issues.append(f"{len(cleanup_targets)} temporary files")
            
            if space_savings > 1024 * 1024:  # > 1MB
                size_str = cleaner._format_size(space_savings)
                cleanliness_issues.append(f"{size_str} of reclaimable space")
            
            health_report["cleanliness"]["issues"] = cleanliness_issues
            health_report["cleanliness"]["status"] = "good" if not cleanliness_issues else "needs_cleanup"
            
        except Exception as e:
            logger.error(f"❌ Cleanliness check failed: {e}")
            health_report["cleanliness"]["status"] = "error"
            health_report["cleanliness"]["issues"] = [str(e)]
        
        # Overall health
        if health_report["structure"]["status"] == "good" and health_report["cleanliness"]["status"] == "good":
            health_report["overall"] = "excellent"
        elif health_report["structure"]["status"] in ["good", "needs_attention"] and health_report["cleanliness"]["status"] in ["good", "needs_cleanup"]:
            health_report["overall"] = "good"
        else:
            health_report["overall"] = "poor"
        
        self._print_health_report(health_report)
        return health_report
    
    def _print_health_report(self, report: dict):
        """Print formatted health report."""
        logger.info("\n📊 PROJECT HEALTH REPORT")
        logger.info("=" * 50)
        
        # Overall status
        overall_emoji = {
            "excellent": "🟢",
            "good": "🟡", 
            "poor": "🔴"
        }
        logger.info(f"Overall Health: {overall_emoji.get(report['overall'], '⚪')} {report['overall'].upper()}")
        
        # Structure status
        structure_emoji = {
            "good": "✅",
            "needs_attention": "⚠️",
            "error": "❌"
        }
        logger.info(f"\nStructure: {structure_emoji.get(report['structure']['status'], '⚪')} {report['structure']['status']}")
        for issue in report["structure"]["issues"]:
            logger.info(f"  - {issue}")
        
        # Cleanliness status
        clean_emoji = {
            "good": "✅",
            "needs_cleanup": "🧹", 
            "error": "❌"
        }
        logger.info(f"\nCleanliness: {clean_emoji.get(report['cleanliness']['status'], '⚪')} {report['cleanliness']['status']}")
        for issue in report["cleanliness"]["issues"]:
            logger.info(f"  - {issue}")
        
        logger.info("=" * 50)
    
    def run_command(self, command: str, **kwargs) -> bool:
        """Execute maintenance command."""
        success = False
        details = ""
        
        try:
            if command == "check":
                health_report = self.check_project_health(verbose=kwargs.get("verbose", False))
                success = health_report["overall"] != "poor"
                details = f"Health: {health_report['overall']}"
            
            elif command == "fix":
                success = self._run_structure_maintenance(**kwargs)
                details = "Structure maintenance"
            
            elif command == "clean":
                success = self._run_cleanup(**kwargs)
                details = "Project cleanup"
            
            elif command == "full":
                # Run both fix and clean
                fix_success = self._run_structure_maintenance(**kwargs)
                clean_success = self._run_cleanup(**kwargs)
                success = fix_success and clean_success
                details = "Full maintenance (fix + clean)"
            
            else:
                logger.error(f"❌ Unknown command: {command}")
                return False
            
        except Exception as e:
            logger.error(f"❌ Command '{command}' failed: {e}")
            details = f"Error: {str(e)}"
            success = False
        
        # Log the maintenance run
        self.log_maintenance_run(command, success, details)
        
        return success
    
    def _run_structure_maintenance(self, **kwargs) -> bool:
        """Run structure maintenance."""
        maintainer = ProjectStructureMaintainer(
            project_root=self.project_root,
            fix_mode=True,
            verbose=kwargs.get("verbose", False)
        )
        return maintainer.run_maintenance()
    
    def _run_cleanup(self, **kwargs) -> bool:
        """Run project cleanup."""
        cleaner = ProjectCleaner(
            project_root=self.project_root,
            aggressive=kwargs.get("aggressive", False),
            dry_run=kwargs.get("dry_run", False)
        )
        return cleaner.run_cleanup()


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Project maintenance management")
    parser.add_argument("command", nargs="?", default="check",
                       choices=["check", "fix", "clean", "full"],
                       help="Maintenance command to run (default: check)")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--aggressive", action="store_true", 
                       help="Aggressive cleanup (for clean/full commands)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Dry run mode (for clean command)")
    
    args = parser.parse_args()
    
    # Initialize manager
    manager = ProjectMaintenanceManager(project_root)
    
    # Welcome message
    logger.info("🛠️  Project Maintenance Tool")
    logger.info(f"📁 Project: {project_root.name}")
    logger.info(f"🎯 Command: {args.command}")
    
    # Run command
    success = manager.run_command(
        args.command,
        verbose=args.verbose,
        aggressive=args.aggressive,
        dry_run=args.dry_run
    )
    
    if success:
        logger.info(f"✅ Maintenance command '{args.command}' completed successfully!")
    else:
        logger.error(f"❌ Maintenance command '{args.command}' failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
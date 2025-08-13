#!/usr/bin/env python3
"""
Script de limpieza automatizada para el proyecto.

Realiza limpieza profunda de:
- Archivos temporales acumulados
- Logs antiguos
- Cachés de Python
- Archivos de backup automáticos
- Directorios vacíos

Usage:
    python tools/scripts/cleanup_project.py [--aggressive] [--dry-run]

Examples:
    # Limpieza básica (solo temporales seguros)
    python tools/scripts/cleanup_project.py
    
    # Limpieza agresiva (incluye logs y cachés)
    python tools/scripts/cleanup_project.py --aggressive
    
    # Ver qué se eliminaría sin hacerlo
    python tools/scripts/cleanup_project.py --dry-run --aggressive
"""

import os
import sys
import argparse
import logging
import shutil
from pathlib import Path
from typing import List, Set
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProjectCleaner:
    """Automated project cleanup utility."""
    
    def __init__(self, project_root: Path, aggressive: bool = False, dry_run: bool = False):
        self.project_root = project_root
        self.aggressive = aggressive
        self.dry_run = dry_run
        
        # Files and patterns to clean (basic mode)
        self.basic_cleanup_patterns = [
            "*.tmp",
            "*.temp", 
            "*~",
            "*.bak",
            "*.backup",
            ".DS_Store",
            "Thumbs.db"
        ]
        
        # Additional patterns for aggressive mode
        self.aggressive_cleanup_patterns = [
            "*.pyc",
            "*.pyo",
            "__pycache__",
            "*.log",
            ".pytest_cache",
            ".coverage",
            "*.orig"
        ]
        
        # Directories to preserve
        self.preserve_dirs = {
            ".git",
            ".venv", 
            "venv",
            "argo-utils-cli",
            "tools",
            "data_analysis",
            "results",
            "logs"  # Preserved in basic mode
        }
    
    def find_cleanup_targets(self) -> List[Path]:
        """Find files and directories to clean up."""
        targets = []
        patterns = self.basic_cleanup_patterns.copy()
        
        if self.aggressive:
            patterns.extend(self.aggressive_cleanup_patterns)
            # In aggressive mode, also clean old logs
            self.preserve_dirs.discard("logs")
        
        logger.info(f"🔍 Scanning for cleanup targets (aggressive: {self.aggressive})...")
        
        # Scan entire project
        for item in self.project_root.rglob('*'):
            if self._should_clean(item, patterns):
                targets.append(item)
        
        # Sort by type for better output
        targets.sort(key=lambda x: (x.is_dir(), x.name))
        
        return targets
    
    def _should_clean(self, path: Path, patterns: List[str]) -> bool:
        """Determine if a path should be cleaned."""
        relative_path = path.relative_to(self.project_root)
        
        # Skip preserved directories and their contents
        for preserve_dir in self.preserve_dirs:
            if preserve_dir in relative_path.parts:
                return False
        
        # Check if matches cleanup patterns
        for pattern in patterns:
            if path.match(pattern) or path.name == pattern:
                return True
        
        # Special cases for aggressive mode
        if self.aggressive:
            # Clean old log files (older than 7 days)
            if path.suffix == '.log':
                try:
                    if datetime.fromtimestamp(path.stat().st_mtime) < datetime.now() - timedelta(days=7):
                        return True
                except Exception:
                    pass
            
            # Clean large temporary CSV files
            if path.suffix == '.csv' and path.stat().st_size > 10 * 1024 * 1024:  # > 10MB
                if any(temp_word in path.name.lower() for temp_word in ['temp', 'tmp', 'test', 'backup']):
                    return True
        
        return False
    
    def clean_targets(self, targets: List[Path]) -> bool:
        """Clean the identified targets."""
        if not targets:
            logger.info("✅ No cleanup targets found - project is clean!")
            return True
        
        # Categorize targets
        files = [t for t in targets if t.is_file()]
        dirs = [t for t in targets if t.is_dir()]
        
        logger.info(f"📋 Found {len(files)} files and {len(dirs)} directories to clean")
        
        if self.dry_run:
            logger.info("🔍 DRY RUN - showing what would be cleaned:")
            self._show_cleanup_preview(files, dirs)
            return True
        
        # Clean files
        cleaned_files = 0
        for file_path in files:
            try:
                file_path.unlink()
                cleaned_files += 1
                logger.info(f"🗑️  Removed file: {file_path.relative_to(self.project_root)}")
            except Exception as e:
                logger.warning(f"⚠️  Could not remove {file_path.name}: {e}")
        
        # Clean directories (empty ones first)
        cleaned_dirs = 0
        for dir_path in sorted(dirs, key=lambda x: len(x.parts), reverse=True):
            try:
                if not any(dir_path.iterdir()):  # Only remove if empty
                    dir_path.rmdir()
                    cleaned_dirs += 1
                    logger.info(f"📁 Removed directory: {dir_path.relative_to(self.project_root)}")
            except Exception as e:
                logger.warning(f"⚠️  Could not remove directory {dir_path.name}: {e}")
        
        logger.info(f"✅ Cleanup completed: {cleaned_files} files, {cleaned_dirs} directories removed")
        return True
    
    def _show_cleanup_preview(self, files: List[Path], dirs: List[Path]):
        """Show preview of what would be cleaned."""
        if files:
            logger.info("📄 Files to be removed:")
            for file_path in files[:10]:  # Show first 10
                size = self._format_size(file_path.stat().st_size)
                logger.info(f"   {file_path.relative_to(self.project_root)} ({size})")
            if len(files) > 10:
                logger.info(f"   ... and {len(files) - 10} more files")
        
        if dirs:
            logger.info("📁 Directories to be removed:")
            for dir_path in dirs[:10]:  # Show first 10
                logger.info(f"   {dir_path.relative_to(self.project_root)}/")
            if len(dirs) > 10:
                logger.info(f"   ... and {len(dirs) - 10} more directories")
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}TB"
    
    def calculate_space_savings(self, targets: List[Path]) -> int:
        """Calculate total space that would be freed."""
        total_size = 0
        
        for path in targets:
            if path.is_file():
                try:
                    total_size += path.stat().st_size
                except Exception:
                    pass
        
        return total_size
    
    def run_cleanup(self) -> bool:
        """Run complete cleanup process."""
        logger.info("🧹 Starting project cleanup...")
        
        if self.dry_run:
            logger.info("👀 DRY RUN mode - no files will be deleted")
        if self.aggressive:
            logger.info("⚡ AGGRESSIVE mode - cleaning caches and logs too")
        
        # Find targets
        targets = self.find_cleanup_targets()
        
        # Calculate space savings
        space_savings = self.calculate_space_savings(targets)
        if space_savings > 0:
            savings_str = self._format_size(space_savings)
            logger.info(f"💾 Potential space savings: {savings_str}")
        
        # Perform cleanup
        success = self.clean_targets(targets)
        
        if success:
            if self.dry_run:
                logger.info("✅ Cleanup preview completed")
            else:
                logger.info("✅ Project cleanup completed successfully!")
        
        return success


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Clean up temporary and unnecessary files")
    parser.add_argument("--aggressive", action="store_true", 
                       help="Include logs and Python cache files")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be cleaned without doing it")
    
    args = parser.parse_args()
    
    # Initialize cleaner
    cleaner = ProjectCleaner(
        project_root=project_root,
        aggressive=args.aggressive,
        dry_run=args.dry_run
    )
    
    # Run cleanup
    success = cleaner.run_cleanup()
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
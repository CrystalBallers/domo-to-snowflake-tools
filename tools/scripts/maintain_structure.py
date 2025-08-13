#!/usr/bin/env python3
"""
Script de automatización para mantener la estructura organizada del proyecto.

Este script verifica y corrige automáticamente:
- Archivos en ubicaciones incorrectas
- Directorios faltantes
- Archivos temporales acumulados
- Estructura de results/ organizada

Usage:
    python tools/scripts/maintain_structure.py [--fix] [--verbose]

Examples:
    # Solo verificar (dry-run)
    python tools/scripts/maintain_structure.py
    
    # Verificar y corregir automáticamente
    python tools/scripts/maintain_structure.py --fix
    
    # Modo verbose para más detalles
    python tools/scripts/maintain_structure.py --fix --verbose
"""

import os
import sys
import argparse
import logging
import shutil
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProjectStructureMaintainer:
    """Maintains the organized project structure."""
    
    def __init__(self, project_root: Path, fix_mode: bool = False, verbose: bool = False):
        self.project_root = project_root
        self.fix_mode = fix_mode
        self.verbose = verbose
        
        # Define expected structure
        self.expected_dirs = [
            "tools/scripts",
            "tools/utils", 
            "data_analysis",
            "results",
            "logs"
        ]
        
        # Define file type rules
        self.file_rules = {
            # Scripts should be in tools/scripts/
            "scripts": {
                "patterns": ["*.py"],
                "exclude_names": ["main.py", "__init__.py"],
                "exclude_dirs": ["tools", "data_analysis", ".venv", ".git"],
                "target_dir": "tools/scripts"
            },
            
            # Analysis scripts in data_analysis/
            "analysis": {
                "patterns": ["*datacompy*.py", "*analysis*.py", "*compare*.py"],
                "target_dir": "data_analysis"
            },
            
            # Results files
            "results": {
                "patterns": ["*.sql"],
                "exclude_names": ["requirements.txt", "credentials.json", ".env"],
                "exclude_dirs": ["tools", "data_analysis", ".venv", ".git", "results"],
                "target_dir": "results"
            },
            
            # CSV files - organized automatically
            "csv_files": {
                "patterns": ["*.csv"],
                "exclude_dirs": ["tools", "data_analysis", ".venv", ".git", "results"],
                "target_dir": "results/csv"
            },
            
            # JSON files - organized automatically  
            "json_files": {
                "patterns": ["*.json"],
                "exclude_names": ["requirements.txt", "credentials.json", ".env", "package.json", "tsconfig.json"],
                "exclude_dirs": ["tools", "data_analysis", ".venv", ".git", "results", "node_modules"],
                "target_dir": "results/json"
            },
            
            # Log files
            "logs": {
                "patterns": ["*.log"],
                "target_dir": "logs"
            }
        }
    
    def check_directory_structure(self) -> List[str]:
        """Check if all expected directories exist."""
        missing_dirs = []
        
        for expected_dir in self.expected_dirs:
            dir_path = self.project_root / expected_dir
            if not dir_path.exists():
                missing_dirs.append(expected_dir)
                logger.warning(f"⚠️  Missing directory: {expected_dir}")
            elif self.verbose:
                logger.info(f"✅ Directory exists: {expected_dir}")
        
        return missing_dirs
    
    def create_missing_directories(self, missing_dirs: List[str]) -> bool:
        """Create missing directories."""
        if not missing_dirs:
            return True
            
        if not self.fix_mode:
            logger.info(f"🔧 Would create {len(missing_dirs)} missing directories (use --fix to apply)")
            return False
        
        try:
            for dir_path in missing_dirs:
                full_path = self.project_root / dir_path
                full_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"📁 Created directory: {dir_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create directories: {e}")
            return False
    
    def find_misplaced_files(self) -> Dict[str, List[Tuple[Path, str]]]:
        """Find files that are in wrong locations."""
        misplaced = {rule_name: [] for rule_name in self.file_rules.keys()}
        
        # Scan project root for misplaced files
        for file_path in self.project_root.iterdir():
            if file_path.is_file():
                self._check_file_placement(file_path, misplaced)
        
        # Scan other directories for misplaced files
        for dir_path in self.project_root.iterdir():
            if dir_path.is_dir() and not dir_path.name.startswith('.'):
                if dir_path.name not in ['tools', 'data_analysis', 'results', 'logs']:
                    for file_path in dir_path.rglob('*'):
                        if file_path.is_file():
                            self._check_file_placement(file_path, misplaced)
        
        return misplaced
    
    def _check_file_placement(self, file_path: Path, misplaced: Dict[str, List[Tuple[Path, str]]]):
        """Check if a single file is properly placed."""
        relative_path = file_path.relative_to(self.project_root)
        
        for rule_name, rule in self.file_rules.items():
            # Check if file matches patterns
            if not any(file_path.match(pattern) for pattern in rule["patterns"]):
                continue
            
            # Check exclusions
            if "exclude_names" in rule and file_path.name in rule["exclude_names"]:
                continue
                
            if "exclude_dirs" in rule:
                if any(part in rule["exclude_dirs"] for part in relative_path.parts[:-1]):
                    continue
            
            # Check if file is in correct location
            target_dir = rule["target_dir"]
            if not str(relative_path).startswith(target_dir):
                misplaced[rule_name].append((file_path, target_dir))
                if self.verbose:
                    logger.warning(f"⚠️  Misplaced {rule_name}: {relative_path} → should be in {target_dir}")
    
    def fix_misplaced_files(self, misplaced: Dict[str, List[Tuple[Path, str]]]) -> bool:
        """Move misplaced files to correct locations."""
        total_files = sum(len(files) for files in misplaced.values())
        
        if total_files == 0:
            logger.info("✅ All files are properly organized")
            return True
        
        if not self.fix_mode:
            logger.info(f"🔧 Would move {total_files} misplaced files (use --fix to apply)")
            for rule_name, files in misplaced.items():
                if files:
                    logger.info(f"  📋 {rule_name}: {len(files)} files")
            return False
        
        try:
            for rule_name, files in misplaced.items():
                for file_path, target_dir in files:
                    target_path = self.project_root / target_dir / file_path.name
                    
                    # Ensure target directory exists
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Move file
                    shutil.move(str(file_path), str(target_path))
                    logger.info(f"📁 Moved {file_path.name} → {target_dir}/")
            
            return True
        except Exception as e:
            logger.error(f"❌ Failed to move files: {e}")
            return False
    
    def clean_empty_directories(self) -> bool:
        """Remove empty directories (except expected ones)."""
        removed_dirs = []
        
        for dir_path in self.project_root.iterdir():
            if dir_path.is_dir() and not dir_path.name.startswith('.'):
                if dir_path.name not in ['tools', 'data_analysis', 'results', 'logs', 'argo-utils-cli']:
                    if self._is_empty_directory(dir_path):
                        if self.fix_mode:
                            try:
                                dir_path.rmdir()
                                removed_dirs.append(dir_path.name)
                                logger.info(f"🗑️  Removed empty directory: {dir_path.name}")
                            except Exception as e:
                                logger.warning(f"⚠️  Could not remove {dir_path.name}: {e}")
                        else:
                            removed_dirs.append(dir_path.name)
        
        if removed_dirs and not self.fix_mode:
            logger.info(f"🔧 Would remove {len(removed_dirs)} empty directories (use --fix to apply)")
        
        return True
    
    def _is_empty_directory(self, dir_path: Path) -> bool:
        """Check if directory is empty or contains only empty subdirectories."""
        try:
            for item in dir_path.iterdir():
                if item.is_file():
                    return False
                elif item.is_dir() and not self._is_empty_directory(item):
                    return False
            return True
        except Exception:
            return False
    
    def organize_results_directory(self) -> bool:
        """Organize files in results/ directory by date/type."""
        results_dir = self.project_root / "results"
        if not results_dir.exists():
            return True
        
        # Create organized structure in results/
        today = datetime.now().strftime("%Y%m%d")
        organized_dirs = {
            "daily": f"daily_{today}",
            "sql": "sql",
            "csv": "csv",
            "json": "json",
            "analysis": "analysis",
            "exports": "exports"
        }
        
        changes_needed = False
        
        # Check for loose files in results/
        for file_path in results_dir.iterdir():
            if file_path.is_file():
                # Determine target subdirectory with new organization
                if file_path.suffix == '.sql':
                    target_subdir = organized_dirs["sql"]
                elif file_path.suffix == '.csv':
                    target_subdir = organized_dirs["csv"] 
                elif file_path.suffix == '.json':
                    target_subdir = organized_dirs["json"]
                else:
                    target_subdir = organized_dirs["daily"]
                
                target_dir = results_dir / target_subdir
                
                if not target_dir.exists() or file_path.parent != target_dir:
                    changes_needed = True
                    if self.fix_mode:
                        target_dir.mkdir(exist_ok=True)
                        target_path = target_dir / file_path.name
                        shutil.move(str(file_path), str(target_path))
                        logger.info(f"📁 Organized: {file_path.name} → results/{target_subdir}/")
                    elif self.verbose:
                        logger.info(f"🔧 Would organize: {file_path.name} → results/{target_subdir}/")
        
        if changes_needed and not self.fix_mode:
            logger.info("🔧 Would organize files in results/ directory (use --fix to apply)")
        
        return True
    
    def run_maintenance(self) -> bool:
        """Run complete maintenance check."""
        logger.info("🚀 Starting project structure maintenance...")
        
        if self.fix_mode:
            logger.info("🔧 Fix mode enabled - changes will be applied")
        else:
            logger.info("👀 Dry-run mode - no changes will be made")
        
        success = True
        
        # 1. Check directory structure
        logger.info("\n📁 Checking directory structure...")
        missing_dirs = self.check_directory_structure()
        if missing_dirs:
            success &= self.create_missing_directories(missing_dirs)
        
        # 2. Find and fix misplaced files
        logger.info("\n📋 Checking file placement...")
        misplaced = self.find_misplaced_files()
        success &= self.fix_misplaced_files(misplaced)
        
        # 3. Clean empty directories
        logger.info("\n🗑️  Checking for empty directories...")
        success &= self.clean_empty_directories()
        
        # 4. Organize results directory
        logger.info("\n📊 Organizing results directory...")
        success &= self.organize_results_directory()
        
        if success:
            logger.info("\n✅ Project structure maintenance completed successfully!")
        else:
            logger.error("\n❌ Some maintenance tasks failed")
        
        return success


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Maintain project structure organization")
    parser.add_argument("--fix", action="store_true", help="Apply fixes (default: dry-run)")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Initialize maintainer
    maintainer = ProjectStructureMaintainer(
        project_root=project_root,
        fix_mode=args.fix,
        verbose=args.verbose
    )
    
    # Run maintenance
    success = maintainer.run_maintenance()
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
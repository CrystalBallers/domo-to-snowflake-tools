#!/usr/bin/env python3
"""
Test runner script for Domo to Snowflake Migration Tools.

This script provides an easy way to run different test suites and
generate comprehensive reports.

Usage:
    python run_tests.py [options]
    
Examples:
    python run_tests.py                    # Run all tests
    python run_tests.py --unit             # Run only unit tests
    python run_tests.py --integration      # Run only integration tests
    python run_tests.py --fast             # Run fast tests only
    python run_tests.py --coverage         # Run with coverage report
    python run_tests.py --parallel         # Run tests in parallel
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path

def run_command(cmd, description=""):
    """Run a command and return success status."""
    print(f"🔄 {description}")
    print(f"   Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"✅ {description} - Success")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} - Failed")
        print(f"   Error: {e.stderr}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Test runner for Domo to Snowflake Migration Tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_tests.py                    # Run all tests
    python run_tests.py --unit             # Run only unit tests  
    python run_tests.py --integration      # Run only integration tests
    python run_tests.py --fast             # Run fast tests only
    python run_tests.py --coverage         # Run with coverage report
    python run_tests.py --parallel         # Run tests in parallel
    python run_tests.py --module cli       # Run tests for specific module
    python run_tests.py --verbose          # Verbose output
    python run_tests.py --no-cov           # Skip coverage reporting
        """
    )
    
    # Test selection options
    parser.add_argument(
        '--unit', 
        action='store_true',
        help='Run only unit tests (fast, isolated)'
    )
    
    parser.add_argument(
        '--integration',
        action='store_true', 
        help='Run only integration tests'
    )
    
    parser.add_argument(
        '--fast',
        action='store_true',
        help='Run only fast tests (excludes slow markers)'
    )
    
    parser.add_argument(
        '--slow',
        action='store_true',
        help='Run only slow tests'
    )
    
    parser.add_argument(
        '--api',
        action='store_true',
        help='Run only API-related tests'
    )
    
    parser.add_argument(
        '--module',
        choices=['cli', 'migration', 'comparison', 'inventory', 'utils', 'maintenance'],
        help='Run tests for specific module'
    )
    
    # Execution options
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Run tests in parallel'
    )
    
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Generate coverage report'
    )
    
    parser.add_argument(
        '--no-cov',
        action='store_true',
        help='Skip coverage reporting'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Verbose output'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be run without executing'
    )
    
    # Output options
    parser.add_argument(
        '--html-report',
        action='store_true',
        help='Generate HTML test report'
    )
    
    parser.add_argument(
        '--xml-report',
        action='store_true',
        help='Generate XML test report (for CI/CD)'
    )
    
    args = parser.parse_args()
    
    # Build pytest command
    cmd = ['python', '-m', 'pytest']
    
    # Add test selection
    if args.unit:
        cmd.extend(['-m', 'unit'])
    elif args.integration:
        cmd.extend(['-m', 'integration'])
    elif args.fast:
        cmd.extend(['-m', 'not slow'])
    elif args.slow:
        cmd.extend(['-m', 'slow'])
    elif args.api:
        cmd.extend(['-m', 'api'])
    elif args.module:
        cmd.append(f'tests/test_{args.module}.py')
    
    # Add execution options
    if args.parallel:
        cmd.extend(['-n', 'auto'])
    
    if args.verbose:
        cmd.append('-v')
    
    # Add coverage options
    if args.coverage or not args.no_cov:
        if not args.no_cov:
            cmd.extend(['--cov=tools', '--cov-report=term-missing'])
            
            if args.html_report:
                cmd.append('--cov-report=html:tests/coverage_html')
            
            if args.xml_report:
                cmd.append('--cov-report=xml:tests/coverage.xml')
    
    # Add report options
    if args.html_report:
        cmd.extend(['--html=tests/report.html', '--self-contained-html'])
    
    if args.xml_report:
        cmd.append('--junitxml=tests/junit.xml')
    
    # Show command if dry run
    if args.dry_run:
        print(f"Would run: {' '.join(cmd)}")
        return 0
    
    # Check if tests directory exists
    if not Path('tests').exists():
        print("❌ Tests directory not found. Make sure you're in the project root.")
        return 1
    
    # Install test dependencies if needed
    if not Path('requirements-test.txt').exists():
        print("⚠️  requirements-test.txt not found. Some tests may fail.")
    else:
        print("📦 Installing test dependencies...")
        install_cmd = [sys.executable, '-m', 'pip', 'install', '-r', 'requirements-test.txt']
        if not run_command(install_cmd, "Installing test dependencies"):
            print("⚠️  Failed to install test dependencies. Continuing anyway...")
    
    # Run tests
    print("\n🧪 Running tests...")
    print("="*80)
    
    success = run_command(cmd, "Running test suite")
    
    if success:
        print("\n🎉 All tests passed!")
        
        # Show coverage report location if generated
        if args.coverage or (not args.no_cov and args.html_report):
            print(f"📊 Coverage report: tests/coverage_html/index.html")
        
        if args.html_report:
            print(f"📄 Test report: tests/report.html")
    else:
        print("\n💥 Some tests failed!")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
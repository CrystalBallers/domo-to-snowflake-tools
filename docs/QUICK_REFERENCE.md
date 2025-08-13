# ⚡ Quick Reference Guide

## 🚀 Advanced Commands

### **Project Maintenance**
```bash
# Complete maintenance with all options
python tools/scripts/project_maintenance.py full --aggressive --dry-run

# Verbose debugging
python tools/scripts/project_maintenance.py check --verbose

# Exit codes for automation
python tools/scripts/project_maintenance.py check --exit-code
```

### **Inventory Management**
```bash
# Export with custom configuration
python main.py inventory \
  --export-dir results/sql/translated_$(date +%Y%m%d) \
  --credentials /path/to/custom-creds.json

# Test connection only
python main.py inventory --test-connection

# Export specific dataflows (if supported)
python main.py inventory --filter "sales,marketing"
```

### **Migration Operations**
```bash
# Batch migration with custom mapping
python main.py migrate \
  --batch-file custom_mapping.json \
  --parallel 4 \
  --skip-validation

# Migration from spreadsheet with filters
python main.py migrate \
  --from-spreadsheet \
  --filter-status "Pending,Failed" \
  --update-status

# Test single dataset before batch
python main.py migrate \
  --dataset-id 12345 \
  --target-table test_table \
  --dry-run
```

### **Dataset Operations**
```bash
# Export all datasets to spreadsheet
python main.py datasets \
  --export-to-spreadsheet \
  --batch-size 500 \
  --include-metadata

# List datasets with filters
python main.py datasets \
  --list-local \
  --filter-name "sales" \
  --format json
```

## 🔧 Power User Tips

### **Terminal Aliases**
```bash
# Add to ~/.bashrc or ~/.zshrc
alias pm='python tools/scripts/project_maintenance.py'
alias domo='python main.py'

# Usage
pm check
pm fix
domo inventory --test-connection
```

### **Environment Management**
```bash
# Multiple environment files
cp .env .env.prod
cp .env .env.dev

# Load specific environment
export ENV=prod && source .env.${ENV} && python main.py inventory
```

### **Parallel Processing**
```bash
# Run multiple exports in parallel
python main.py inventory --export-dir batch1 &
python main.py migrate --dataset-id 123 --target-table table1 &
wait  # Wait for all background jobs
```

### **Output Formatting**
```bash
# JSON output for automation
python main.py datasets --list-local --format json > datasets.json

# CSV export
python main.py inventory --format csv > inventory.csv

# Silent mode for cron jobs
python main.py migrate --quiet --log-file /var/log/migration.log
```

## 📊 Monitoring & Logging

### **Advanced Logging**
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python main.py inventory

# Custom log format
export LOG_FORMAT='%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Log to specific file
python main.py migrate --log-file migration_$(date +%Y%m%d).log
```

### **Health Checks**
```bash
# Complete system health check
python tools/scripts/project_maintenance.py check --comprehensive

# Performance benchmarks
python tools/scripts/project_maintenance.py check --benchmark

# Memory usage monitoring
python -m memory_profiler main.py inventory
```

### **Error Analysis**
```bash
# Extract errors from logs
grep "ERROR\|CRITICAL" logs/*.log | tail -20

# Count operations by type
grep -c "SUCCESS\|FAILED" logs/migration.log

# Show recent failures
grep "FAILED" logs/*.log | tail -10 | cut -d' ' -f3-
```

## 🔄 Automation Patterns

### **Pre-commit Hooks**
```bash
#!/bin/sh
# .git/hooks/pre-commit
python tools/scripts/project_maintenance.py check --exit-code
if [ $? -ne 0 ]; then
    echo "❌ Project maintenance check failed"
    exit 1
fi
```

### **Cron Automation**
```bash
# Weekly full maintenance
0 3 * * 0 cd /app && python tools/scripts/project_maintenance.py full

# Daily inventory sync
0 9 * * * cd /app && python main.py inventory --sync-only

# Hourly health check
0 * * * * cd /app && python tools/scripts/project_maintenance.py check --quiet
```

### **CI/CD Integration**
```yaml
# GitHub Actions workflow
- name: Run maintenance
  run: |
    python tools/scripts/project_maintenance.py check --exit-code
    python main.py inventory --test-connection
    python main.py migrate --test-connection
```

## 🚨 Emergency Commands

### **"Project is broken!"**
```bash
# Reset to clean state
python tools/scripts/project_maintenance.py full --aggressive --force

# Check what's wrong
python tools/scripts/project_maintenance.py check --verbose --comprehensive
```

### **"Migration stuck!"**
```bash
# Kill stuck processes
pkill -f "python main.py migrate"

# Check locks
find . -name "*.lock" -delete

# Restart with recovery
python main.py migrate --resume --recovery-mode
```

### **"Out of space!"**
```bash
# Emergency cleanup
python tools/scripts/cleanup_project.py --aggressive --emergency

# Find large files
find . -size +100M -type f | head -10

# Clean temporary directories
rm -rf /tmp/domo_*
```

### **"Connections failing!"**
```bash
# Test all connections
python main.py inventory --test-connection --verbose
python main.py migrate --test-connection --verbose
python main.py datasets --test-connection --verbose

# Reset connection cache
rm -rf ~/.cache/domo_migration/
```

## 📋 Configuration Tricks

### **Dynamic Configuration**
```bash
# Environment-specific settings
export SNOWFLAKE_WAREHOUSE=${ENV}_WH
export EXPORT_DIR=results/${ENV}/sql/translated

# Runtime configuration
python main.py inventory \
  --config-override "snowflake.timeout=300" \
  --config-override "domo.batch_size=50"
```

### **Performance Tuning**
```bash
# Increase timeouts for large datasets
export DOMO_TIMEOUT=600
export SNOWFLAKE_TIMEOUT=300

# Optimize batch sizes
export BATCH_SIZE=100
export PARALLEL_WORKERS=4

# Memory optimization
export PYTHONHASHSEED=0
export POLARS_MAX_THREADS=4
```

### **Security Hardening**
```bash
# Use credential helpers
export GOOGLE_SHEETS_CREDENTIALS_FILE=$(credential-helper gsheets)
export SNOWFLAKE_PASSWORD=$(credential-helper snowflake)

# Audit mode
python main.py migrate --audit-mode --no-execute --log-all-queries
```

## 🎯 Productivity Hacks

### **Custom Scripts**
```bash
# Create custom workflow script
cat > quick_migration.sh << 'EOF'
#!/bin/bash
set -e
echo "🚀 Starting quick migration workflow..."
python tools/scripts/project_maintenance.py check
python main.py inventory --test-connection
python main.py migrate --from-spreadsheet --filter-status Pending
echo "✅ Workflow completed!"
EOF
chmod +x quick_migration.sh
```

### **Data Validation**
```bash
# Compare row counts before/after migration
python main.py compare --dataset-id 123 --table sales_data --quick-check

# Validate schema mapping
python main.py validate --mapping-file dataset_mapping.json

# Generate migration report
python main.py report --migration-summary --format html > report.html
```

### **Bulk Operations**
```bash
# Process multiple spreadsheets
for sheet in sheet1 sheet2 sheet3; do
  python main.py migrate --from-spreadsheet --sheet-name $sheet
done

# Batch test connections
python main.py test-all-connections --format table
```

This reference provides advanced commands and patterns for power users who need maximum efficiency and control.
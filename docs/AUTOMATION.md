# 🤖 Project Automation Advanced Guide

## 🎯 Advanced Automation Features

This guide covers advanced automation features for users who want to set up continuous maintenance and monitoring.

## 🔄 Automated Maintenance Scripts

### 1. **`project_maintenance.py`** - Control Center
**Advanced commands:**
```bash
# Complete maintenance with aggressive cleanup
python tools/scripts/project_maintenance.py full --aggressive

# Dry run to see what would be changed
python tools/scripts/project_maintenance.py fix --dry-run

# Verbose output for debugging
python tools/scripts/project_maintenance.py check --verbose
```

### 2. **CI/CD Integration**

#### GitHub Actions Example:
```yaml
name: Project Maintenance
on:
  schedule:
    - cron: '0 9 * * 1'  # Every Monday at 9 AM
  workflow_dispatch:

jobs:
  maintain:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run maintenance
        run: python tools/scripts/project_maintenance.py check
```

#### GitLab CI Example:
```yaml
maintenance:
  stage: maintain
  script:
    - python tools/scripts/project_maintenance.py check
  schedule:
    - cron: "0 9 * * 1"
  only:
    - schedules
```

### 3. **Cron Automation**
```bash
# Weekly health check (Monday 9 AM)
0 9 * * 1 cd /path/to/project && python tools/scripts/project_maintenance.py check

# Daily light cleanup (6 PM)
0 18 * * * cd /path/to/project && python tools/scripts/project_maintenance.py clean

# Monthly deep cleanup (first Sunday)
0 3 1-7 * 0 cd /path/to/project && python tools/scripts/project_maintenance.py full --aggressive
```

## 📊 Advanced Health Monitoring

### Custom Health Reports
```python
# Custom health check script
from tools.scripts.project_maintenance import ProjectMaintenance

pm = ProjectMaintenance()
health = pm.check_project_health()

# Send to monitoring system
if health['overall'] != 'excellent':
    send_alert(f"Project health: {health['overall']}")
```

### Integration with Monitoring Tools
```bash
# Prometheus metrics
echo "project_health{status=\"${health}\"} 1" | curl -X POST http://pushgateway:9091/metrics/job/domo-migration

# Slack notifications
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"🚨 Project needs attention: '"$health"'"}' \
  $SLACK_WEBHOOK_URL
```

## 🔧 Customization

### Add Custom Structure Rules
```python
# In tools/scripts/maintain_structure.py
self.file_rules.update({
    "custom_analysis": {
        "patterns": ["*_analysis.py", "*_report.py"],
        "target_dir": "reports",
        "description": "Analysis and report scripts"
    },
    "data_exports": {
        "patterns": ["export_*.csv", "dump_*.json"],
        "target_dir": "results/exports",
        "description": "Data export files"
    }
})
```

### Custom Cleanup Patterns
```python
# In tools/scripts/cleanup_project.py
self.custom_patterns = [
    "*.backup",
    "*.old",
    "*_temp_*",
    "debug_*.log"
]
```

## 📈 Performance Optimization

### Parallel Processing
```python
# Enable parallel file processing
python tools/scripts/project_maintenance.py fix --parallel

# Adjust worker count
python tools/scripts/project_maintenance.py clean --workers 4
```

### Large Project Handling
```bash
# For projects with 10k+ files
python tools/scripts/project_maintenance.py check --batch-size 1000

# Skip expensive operations
python tools/scripts/project_maintenance.py fix --skip-validation
```

## 🚨 Alert Configuration

### Email Notifications
```python
# Add to project_maintenance.py
import smtplib
from email.mime.text import MIMEText

def send_health_alert(health_status):
    if health_status in ['poor', 'critical']:
        msg = MIMEText(f"Project health: {health_status}")
        msg['Subject'] = '🚨 Domo Migration Project Alert'
        # Send email logic
```

### Dashboard Integration
```python
# Export metrics for dashboards
import json

def export_metrics():
    metrics = {
        "timestamp": datetime.now().isoformat(),
        "health": check_project_health(),
        "file_count": count_project_files(),
        "cleanup_savings": calculate_cleanup_savings()
    }
    
    with open("metrics.json", "w") as f:
        json.dump(metrics, f)
```

## 🎯 Production Deployment

### Docker Integration
```dockerfile
# Add to Dockerfile
COPY tools/scripts/ /app/tools/scripts/
RUN chmod +x /app/tools/scripts/project_maintenance.py

# Add cron job
RUN echo "0 9 * * 1 cd /app && python tools/scripts/project_maintenance.py check" | crontab -
```

### Kubernetes CronJob
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: project-maintenance
spec:
  schedule: "0 9 * * 1"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: maintenance
            image: domo-migration:latest
            command: ["python", "tools/scripts/project_maintenance.py", "check"]
          restartPolicy: OnFailure
```

## 📝 Maintenance Logs Analysis

### Log Parsing
```bash
# Extract error patterns
grep "ERROR" logs/maintenance.log | head -10

# Count maintenance operations
grep -c "SUCCESS" logs/maintenance.log

# Show recent activity
tail -f logs/maintenance.log
```

### Automated Log Rotation
```bash
# Add to crontab
0 0 * * 0 find logs/ -name "*.log" -mtime +30 -delete
```

## 🔄 Advanced Workflows

### Pre-commit Hooks
```bash
#!/bin/sh
# .git/hooks/pre-commit
python tools/scripts/project_maintenance.py check --exit-code
```

### Release Automation
```bash
# Before release
python tools/scripts/project_maintenance.py full --aggressive
python tools/scripts/project_maintenance.py check --strict
```

This guide provides enterprise-level automation capabilities for maintaining project health and structure automatically.
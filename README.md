# BACPAC Model Sync Tool

A Python tool to merge SQL Server bacpac model.xml with a base model, removing backup tables and cleaning HangFire data.

## Features
- Auto export bacpac from Azure SQL Database
- Merge bacpac model with base model.xml
- Remove backup tables (patterns: `_BK_`, `_SL_BK_`)
- Clean HangFire data to avoid FK constraint issues
- Docker support

## Usage

### Docker
```bash
# Run Every time theres a changes in Script/Code
docker-compose down --remove-orphans
docker-compose build --no-cache  
# Run Backup/Model Builder
docker-compose run --rm model-sync
```

### Environment Variables
| Variable | Description |
|----------|-------------|
| `AUTO_EXPORT` | Set to `true` to export from Azure |
| `AZURE_SERVER` | Azure SQL server |
| `AZURE_DATABASE` | Database name |
| `AZURE_USERNAME` | SQL username |
| `AZURE_PASSWORD` | SQL password |
| `BACPAC_FILE` | Path to bacpac file |
| `MODEL_FILE` | Path to base model.xml |
| `OUTPUT_DIR` | Output directory |

# Project Structure

## Data Management System Repository

```
financial-data-management/
├── README.md
├── .gitignore
├── .env.example
├── requirements.txt
├── requirements-dev.txt
├── pyproject.toml              # Python project configuration
├── docker-compose.yml
├── Dockerfile
│
├── config/                     # Configuration files
│   ├── tables.yaml            # Snowflake table sync configuration
│   ├── pipeline.yaml          # Pipeline configuration
│   └── environments/          # Environment-specific configs
│       ├── dev.yaml
│       ├── staging.yaml
│       └── prod.yaml
│
├── pipeline/                   # Core ETL pipeline
│   ├── __init__.py
│   ├── main.py                # Pipeline entry point
│   ├── config/
│   │   ├── __init__.py
│   │   ├── settings.py        # Configuration management
│   │   ├── database.py        # Database connection configs
│   │   ├── table_config.py    # Table configuration loader
│   │   └── logging.py         # Logging configuration
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── snowflake_extractor.py
│   │   ├── metadata_extractor.py
│   │   └── base_extractor.py
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── data_processor.py
│   │   ├── encryptor.py
│   │   ├── compressor.py
│   │   └── validator.py
│   ├── loaders/
│   │   ├── __init__.py
│   │   ├── postgres_loader.py
│   │   └── base_loader.py
│   ├── orchestrator/
│   │   ├── __init__.py
│   │   ├── pipeline_manager.py
│   │   ├── scheduler.py
│   │   └── monitor.py
│   ├── tasks/                 # Celery tasks
│   │   ├── __init__.py
│   │   ├── celery_app.py
│   │   ├── pipeline_tasks.py
│   │   └── maintenance_tasks.py
│   └── utils/
│       ├── __init__.py
│       ├── helpers.py
│       ├── validators.py
│       ├── exceptions.py
│       └── constants.py
│
├── api/                       # FastAPI REST API
│   ├── __init__.py
│   ├── main.py               # FastAPI application entry point
│   ├── dependencies.py       # Dependency injection
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── pipeline.py       # Pipeline management endpoints
│   │   ├── data.py          # Data access endpoints
│   │   ├── metadata.py      # Metadata endpoints
│   │   ├── tables.py        # Table configuration endpoints
│   │   ├── health.py        # Health check endpoints
│   │   └── admin.py         # Admin endpoints
│   ├── models/
│   │   ├── __init__.py
│   │   ├── pipeline.py      # Pydantic models for pipeline
│   │   ├── data.py          # Data models
│   │   ├── metadata.py      # Metadata models
│   │   ├── table_config.py  # Table configuration models
│   │   └── responses.py     # API response models
│   ├── middleware/
│   │   ├── __init__.py
│   │   ├── auth.py          # API authentication
│   │   ├── cors.py          # CORS handling
│   │   ├── logging.py       # Request logging
│   │   └── error_handler.py # Global error handling
│   └── services/
│       ├── __init__.py
│       ├── pipeline_service.py
│       ├── data_service.py
│       ├── metadata_service.py
│       └── table_config_service.py
│
├── tests/                    # Test suite
│   ├── __init__.py
│   ├── conftest.py          # Pytest configuration
│   ├── test_pipeline/
│   │   ├── __init__.py
│   │   ├── test_extractors.py
│   │   ├── test_transformers.py
│   │   └── test_loaders.py
│   ├── test_api/
│   │   ├── __init__.py
│   │   ├── test_routes.py
│   │   └── test_services.py
│   └── fixtures/
│       ├── sample_data.json
│       ├── test_schemas.sql
│       └── sample_tables.yaml
│
├── scripts/                  # Utility scripts
│   ├── setup.py             # Environment setup
│   ├── migrate_db.py        # Database migrations
│   ├── backup_data.py       # Data backup utilities
│   ├── sync_tables.py       # Table configuration sync utility
│   └── deploy.sh            # Deployment script
│
├── docs/                     # Documentation
│   ├── api.md               # API documentation
│   ├── pipeline.md          # Pipeline documentation
│   ├── table-configuration.md # Table sync configuration guide
│   ├── deployment.md        # Deployment guide
│   └── troubleshooting.md   # Common issues and solutions
│
└── monitoring/               # Monitoring and observability
    ├── prometheus.yml       # Prometheus configuration
    ├── grafana/
    │   └── dashboards/
    └── alerts/
        └── pipeline_alerts.yml
```

## Key Conventions

### File Naming & Structure
- Use `snake_case` for Python files and directories
- Follow Python package structure with `__init__.py` files
- Group related functionality in modules (extractors, transformers, loaders)
- Separate business logic from API logic

### Module Organization
- **ETL Pattern**: Clear separation of Extract → Transform → Load operations
- **Service Layer**: Business logic in services, called by API routes
- **Configuration**: Centralized in `config/` with environment-specific settings
- **Task Queue**: Celery tasks for asynchronous pipeline operations

### API Design
- **RESTful Endpoints**: Follow REST conventions for resource management
- **Pydantic Models**: Strong typing for request/response validation
- **Error Handling**: Consistent error responses with proper HTTP status codes
- **Authentication**: API key or JWT-based authentication for external access

### Data Flow Architecture
1. **Pipeline Execution**: `main.py` → `orchestrator` → `extractors` → `transformers` → `loaders`
2. **API Requests**: `routes` → `services` → `pipeline` modules → database
3. **Background Tasks**: `Celery tasks` → `pipeline` modules → database
4. **Monitoring**: `prometheus metrics` → `Grafana dashboards`

### Environment Management
- **Development**: Local PostgreSQL, Redis for Celery
- **Staging**: Docker containers with external databases
- **Production**: Kubernetes or Docker Swarm deployment
- **Secrets**: Environment variables for all sensitive configuration

### Testing Strategy
- **Unit Tests**: Individual component testing with mocks
- **Integration Tests**: Database and external service integration
- **API Tests**: FastAPI test client for endpoint validation
- **Pipeline Tests**: End-to-end pipeline execution with test data

### Deployment & Operations
- **Containerization**: Docker for consistent environments
- **Process Management**: Separate containers for API, workers, scheduler
- **Monitoring**: Prometheus metrics, structured logging, health checks
- **Backup**: Automated database backups and data archival

## Table Configuration Management

### Primary Configuration: `config/tables.yaml`
This is where you specify the list of Snowflake tables to sync:

```yaml
# Example structure for tables.yaml
tables:
  - name: "financial_data"
    snowflake:
      database: "PROD_DB"
      schema: "PUBLIC"
      table: "FINANCIAL_DATA"
    postgres:
      schema: "public"
      table: "financial_data"
    sync_mode: "full"  # full, incremental
    schedule: "daily"
    encryption: true
    compression: true
    
  - name: "market_data"
    snowflake:
      database: "PROD_DB"
      schema: "PUBLIC"
      table: "MARKET_DATA"
    postgres:
      schema: "public"
      table: "market_data"
    sync_mode: "incremental"
    incremental_column: "updated_at"
    schedule: "hourly"
    encryption: true
    compression: false
```

### Configuration Management Options:

1. **File-based (Recommended)**: Edit `config/tables.yaml` directly
2. **API-based**: Use `/api/v1/tables/` endpoints to manage table configurations
3. **Environment-specific**: Override settings in `config/environments/{env}.yaml`
4. **Script-based**: Use `scripts/sync_tables.py` for bulk configuration updates

### Table Configuration Components:
- **`pipeline/config/table_config.py`**: Loads and validates table configurations
- **`api/routes/tables.py`**: API endpoints for table configuration management
- **`api/services/table_config_service.py`**: Business logic for table configuration
- **`api/models/table_config.py`**: Pydantic models for table configuration validation
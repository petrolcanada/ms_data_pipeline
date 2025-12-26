# Technology Stack

## Data Management System (Python)
- **Language**: Python 3.9+
- **Framework**: FastAPI for REST API development
- **Database Connectors**: 
  - `snowflake-connector-python` for Snowflake access
  - `psycopg2-binary` or `asyncpg` for PostgreSQL
- **Data Processing**: 
  - `pandas` for data manipulation and analysis
  - `numpy` for numerical operations
  - `pyarrow` for efficient data serialization
- **Security & Compression**: 
  - `cryptography` library for data encryption
  - `gzip`, `lz4`, or `zstandard` for data compression
- **Job Scheduling**: 
  - `celery` with Redis/RabbitMQ for distributed task processing
  - `APScheduler` for cron-like scheduling
- **API & Web Framework**: 
  - `FastAPI` for high-performance REST APIs
  - `uvicorn` as ASGI server
  - `pydantic` for data validation and serialization
- **Environment Management**: 
  - `python-dotenv` for environment variables
  - **Conda** for dependency management and virtual environments
- **Monitoring & Logging**: 
  - `structlog` for structured logging
  - `prometheus-client` for metrics
  - `sentry-sdk` for error tracking

## Infrastructure
- **Databases**: 
  - Snowflake (source, VPN-hosted)
  - PostgreSQL (target, external)
- **Message Queue**: Redis or RabbitMQ for Celery
- **Networking**: VPN tunnel management for secure Snowflake access
- **Deployment**: Docker containers with Python runtime
- **Process Management**: `gunicorn` for production API deployment

## Common Commands

### Development Setup
```bash
# Create conda environment
conda env create -f environment.yml

# Or for development with extra tools
conda env create -f environment-dev.yml

# Activate environment
conda activate financial-data-management

# Environment setup
cp .env.example .env
# Edit .env with your database credentials
```

### Environment Management
```bash
# Update environment from file
conda env update -f environment.yml

# Install additional packages
conda install package-name
pip install package-name  # for packages not in conda

# Export current environment
conda env export > environment-backup.yml

# List environments
conda env list

# Remove environment
conda env remove -n financial-data-management
```

### Pipeline Operations
```bash
# Run metadata extraction
python -m pipeline.extract_metadata --table <table_name>

# Run full pipeline for a table
python -m pipeline.main --table <table_name> --mode full

# Run incremental pipeline
python -m pipeline.main --table <table_name> --mode incremental

# Schedule pipeline jobs
python -m pipeline.scheduler --schedule daily
```

### API Server
```bash
# Development server
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# Production server
gunicorn api.main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### Background Workers
```bash
# Start Celery worker
celery -A pipeline.tasks worker --loglevel=info

# Start Celery beat scheduler
celery -A pipeline.tasks beat --loglevel=info

# Monitor Celery tasks
celery -A pipeline.tasks flower
```

### Testing & Quality
```bash
# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=pipeline --cov=api --cov-report=html

# Code formatting
black pipeline/ api/ tests/
isort pipeline/ api/ tests/

# Linting
flake8 pipeline/ api/ tests/
mypy pipeline/ api/
```

## Development Guidelines
- **Code Style**: Follow PEP 8, use `black` for formatting, `isort` for imports
- **Type Hints**: Use type hints throughout, validate with `mypy`
- **Error Handling**: Implement comprehensive exception handling for network and database operations
- **Logging**: Use structured logging with appropriate log levels
- **Testing**: Write unit tests for business logic, integration tests for database operations
- **Security**: Store credentials in environment variables, use encryption for sensitive data
- **Documentation**: Document API endpoints with FastAPI's automatic OpenAPI generation
- **Performance**: Use async/await for I/O operations, implement connection pooling
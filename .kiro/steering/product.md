# Product Overview

This repository contains the **Data Management System** for financial data processing and API services.

## Data Management System (Python)
- **Purpose**: Complete data pipeline and management API for financial data processing
- **Components**:
  - **Data Pipeline**: ETL processes to migrate data from Snowflake (VPN-hosted) to PostgreSQL (external)
  - **Data API**: RESTful endpoints for data access and pipeline management
  - **Pipeline Orchestration**: Scheduling and monitoring of data processing jobs

## Key Features
- **Cross-network Data Synchronization**: Secure data transfer from VPN-hosted Snowflake to external PostgreSQL
- **Automated Metadata Extraction**: Dynamic table schema discovery from Snowflake
- **Data Security**: Encryption and compression for large financial datasets
- **Pipeline Management**: Ad-hoc and scheduled data pipeline execution
- **API Services**: RESTful endpoints for external systems (like the separate website repository)
- **Monitoring & Logging**: Comprehensive pipeline monitoring and error handling

## Architecture Goals
- **Robust ETL Processing**: Reliable data extraction, transformation, and loading
- **Secure Data Handling**: Encryption in transit and at rest for sensitive financial data
- **Scalable Pipeline**: Handle large volumes of data efficiently
- **API-First Design**: Provide clean REST APIs for data consumption by external systems
- **Operational Excellence**: Comprehensive logging, monitoring, and error recovery
# San Francisco Eviction Notices ETL Pipeline

## Overview

This project implements a comprehensive ETL (Extract, Transform, Load) pipeline for analyzing San Francisco eviction notices data. The pipeline leverages Apache Airflow for orchestration, AWS services for data storage and processing, and AWS QuickSight for business intelligence visualization.

## Architecture

```
CSV Data (S3) → Airflow DAG → Data Transformation → PostgreSQL (RDS) → QuickSight Dashboard
```

### Technology Stack

- **Orchestration**: Apache Airflow
- **Programming Language**: Python 3.x
- **Cloud Platform**: Amazon Web Services (AWS)
- **Data Storage**: AWS S3 (raw data), AWS RDS PostgreSQL (processed data)
- **Visualization**: AWS QuickSight
- **Environment Management**: Conda

## Data Source

The pipeline processes eviction notice data filed with the San Francisco Rent Board per San Francisco Administrative Code 37.9(c). 

**Key Data Characteristics:**
- **Format**: CSV files
- **Coverage**: January 1, 1997 - Present
- **Important Note**: Notices represent filed evictions, not necessarily completed evictions

## Project Objectives

The ETL pipeline enables analysis of:
- **Eviction Trends**: Identification of most and least common reasons for evictions
- **Geographic Analysis**: Neighborhoods with highest eviction rates
- **Temporal Patterns**: Eviction trends over time

## Installation & Setup

### Prerequisites

```bash
# Create conda virtual environment
conda create -n sf-evictions python=3.x
conda activate sf-evictions
```

### Required Dependencies

```bash
pip install apache-airflow
pip install pandas
pip install sqlalchemy
pip install boto3
pip install python-dotenv
pip install psycopg2-binary
```

### Environment Configuration

Create a `.env` file with the following variables:

```env
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=your_region

# S3 Configuration
S3_BUCKET_NAME=your_s3_bucket
S3_FILE_KEY=path/to/eviction_data.csv

# RDS Configuration
RDS_ENDPOINT=your_rds_endpoint
RDS_DATABASE=your_database_name
RDS_USERNAME=your_username
RDS_PASSWORD=your_password
RDS_PORT=5432
```

## Pipeline Architecture

### 1. Extract Phase
- **Tool**: Boto3
- **Function**: Data extraction from AWS S3 bucket
- **Input**: Raw CSV files containing eviction notices
- **Output**: Pandas DataFrame

### 2. Transform Phase
- **Tool**: Pandas
- **Operations**: 
  - Data cleaning and validation
  - Data type conversions
  - Handling missing values
  - Feature engineering for analysis
- **Output**: Cleaned and structured DataFrame

### 3. Load Phase
- **Tool**: SQLAlchemy
- **Target**: AWS RDS PostgreSQL database
- **Operation**: Efficient bulk loading of transformed data
- **Schema**: Optimized for analytical queries

## Airflow DAG Structure

The pipeline is orchestrated through an Airflow DAG with the following tasks:

```python
dag = DAG(
    'sf_evictions_etl',
    description='ETL pipeline for SF eviction notices',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

extract_task >> transform_task >> load_task
```

### Task Dependencies
1. **extract_s3_data**: Extracts CSV data from S3 bucket
2. **transform_data**: Cleans and processes the raw data
3. **load_to_rds**: Loads transformed data into PostgreSQL

## Data Visualization

### AWS QuickSight Integration
- **Data Source**: AWS RDS PostgreSQL
- **Connection**: Direct connection to transformed dataset
- **Dashboards**: Interactive visualizations for business analysis

### Key Visualizations
- Eviction reasons frequency analysis
- Geographic distribution heatmaps
- Time series trend analysis
- Neighborhood comparison charts

## Database Schema

The PostgreSQL database contains optimized tables for:
- Eviction notices with standardized fields
- Geographic information for neighborhood analysis
- Temporal data for trend analysis
- Reference tables for eviction reasons

## Deployment

### Local Development
```bash
# Start Airflow webserver
airflow webserver --port 8080

# Start Airflow scheduler
airflow scheduler
```

### Production Considerations
- Implement proper IAM roles for AWS resource access
- Use AWS Secrets Manager for sensitive credentials
- Configure Airflow with appropriate resource limits
- Set up monitoring and alerting for pipeline failures

## Monitoring & Logging

- **Airflow UI**: Monitor DAG execution and task status
- **CloudWatch**: AWS service monitoring and logging
- **Data Quality**: Automated data validation checks
- **Error Handling**: Comprehensive exception handling and retry logic

## Security

- Environment variables for sensitive configuration
- IAM roles with least privilege access
- Encrypted connections to RDS
- S3 bucket access controls

## Future Enhancements

- Implement incremental data loading
- Add data quality monitoring
- Expand visualization capabilities
- Integrate machine learning models for predictive analysis

## Contributing

Please ensure all code follows PEP 8 standards and includes appropriate documentation and testing.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Note**: This pipeline processes public data from the San Francisco Rent Board. Eviction notices do not necessarily indicate completed evictions.

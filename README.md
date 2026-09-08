# CloudDataLakehouse

## Project Overview

**CloudDataLakehouse** is a comprehensive, serverless data lakehouse platform built on **AWS Cloud Development Kit (CDK)** that ingests, transforms, and analyzes data from multiple sources (Hacker News and Twitter) using a modern medallion architecture (Bronze → Silver → Gold layers). The platform provides automated data collection, ETL processing, and business intelligence capabilities with a complete infrastructure-as-code implementation.

## Key Features

### Data Sources
- **Hacker News**: Real-time collection of stories, comments, jobs, and polls from the Hacker News community via Algolia API
- **Twitter/Bitcoin Data**: Historical and real-time Bitcoin-related tweets with user engagement metrics

### Architecture Pattern (Medallion Architecture)

The project implements the industry-standard three-tier data lakehouse pattern:

#### **Bronze Layer (Raw Data)**
- Raw, unprocessed data ingested directly from source APIs
- Stored in S3 with full versioning and encryption
- Serves as the single source of truth for all data ingestion
- Data collected from:
  - Hacker News Algolia API (stories, comments, jobs, polls)
  - Twitter/Bitcoin datasets (CSV format)

#### **Silver Layer (Cleaned & Normalized)**
- Cleaned, deduplicated, and standardized data
- Removed invalid records, HTML tags, and formatting issues
- Applied consistent schemas and data types
- Partitioned by year and month for optimized querying
- Separated entities:
  - **Users**: Deduplicated user profiles with metadata
  - **Posts**: Standardized posts with parsed timestamps and normalized fields

#### **Gold Layer (Analytics Ready)**
- Aggregated, business-aligned data for reporting and analytics
- Pre-computed metrics and KPIs:
  - **Hacker News Metrics**:
    - Daily post type distribution
    - Top 10 users by karma (highest and lowest)
    - Top 10 job posts by score
    - Top 10 stories by score
    - Data quality scores
  - **Twitter Metrics**:
    - Daily user counts
    - Top 10 users by followers
    - Data quality scores
- Loaded into PostgreSQL for real-time SQL analytics
- Optimized for BI tools and dashboards

### AWS Services & Technologies

#### Compute & Orchestration
- **AWS Lambda**: Serverless functions for data collection, transformation, and loading
  - `HackerNewsCollector`: Fetches HN data via Algolia API with recursive interval splitting for complete data coverage
  - `TwitterSilverLambda`: Transforms Twitter data into normalized Silver layer
  - `GoldPostgresLoader`: Loads Gold layer metrics into PostgreSQL
- **AWS EventBridge**: Scheduled event triggers for periodic data collection (cron schedules)
- **AWS CloudWatch**: Metrics, logs, and alarms for monitoring

#### Storage
- **Amazon S3**: Multi-layer data lake storage with:
  - Versioning enabled for data recovery
  - Server-side encryption (S3-managed)
  - Block public access policies
  - Organized partition structure (year/month)
- **Amazon PostgreSQL (EC2-hosted)**: Analytical database for Gold layer data and reporting

#### Networking & Security
- **Amazon VPC**: Private Virtual Private Cloud with isolated subnets
- **Security Groups**: Fine-grained network access controls for Lambda and EC2
- **AWS Secrets Manager**: Secure management of database credentials
- **IAM Roles & Policies**: Least privilege access for all services

#### Infrastructure
- **AWS EC2**: Managed PostgreSQL database instance for analytics
- **AWS CDK**: Infrastructure as Code for reproducible deployments
- **AWS SNS**: Notifications and alerting

### Technology Stack

- **Language**: Python 3
- **Infrastructure**: AWS CDK 2.142.1
- **Data Processing**: 
  - Pandas 3.0.3 (data manipulation)
  - AWS SDK for Pandas (awswrangler 3.17.0) (S3 integration)
  - NumPy (numerical operations)
- **Database**: PostgreSQL with psycopg2 binary driver
- **AWS SDK**: Boto3 (AWS service interactions)

### Data Flow Pipeline

```
1. BRONZE LAYER (Data Collection)
   ├─ HackerNews Collector Lambda
   │  └─ Algolia API → Raw JSON → S3 Bronze
   └─ Twitter Data (CSV) → S3 Bronze

2. SILVER LAYER (Data Cleaning & Transformation)
   ├─ HackerNews Normalizer Lambda
   │  └─ Parse, deduplicate, normalize → S3 Silver (users/ & posts/)
   └─ Twitter Silver Lambda
      └─ Parse dates, clean HTML tags, normalize → S3 Silver (users/ & posts/)

3. GOLD LAYER (Analytics & Aggregation)
   ├─ HackerNews Transformator Lambda
   │  └─ Aggregate metrics, compute KPIs
   └─ Twitter Gold Lambda
      └─ Aggregate metrics, compute KPIs
   └─ Gold Postgres Loader
      └─ Load metrics into PostgreSQL for real-time analytics

4. NOTIFICATION LAYER
   └─ Discord Notifier: Status updates and alerts
```

### Project Structure

```
CloudDataLakehouse/
├── app.py                           # CDK Application Entry Point
├── requirements.txt                 # Python Dependencies
├── cdk.json                        # CDK Configuration
│
├── Network Infrastructure
│   └── network_stack/
│       └── stack.py               # VPC, Subnets, Security Groups
│
├── Database & Secrets
├── db_secret_stack/
│   └── stack.py                   # RDS Secrets Manager
└── ec2_stack/
    └── stack.py                   # PostgreSQL EC2 Instance
│
├── Data Collection (Bronze Layer)
├── hacker_news_bronze_stack/
│   └── stack.py                   # HN Collection Lambda Setup
├── hacker_news_collector/
│   └── handler.py                 # Lambda: Fetch HN Data
├── twitter_silver_stack/
│   └── stack.py                   # Twitter Transformation Setup
└── twitter_silver_lambda/
    └── handler.py                 # Lambda: Transform Twitter Data
│
├── Data Transformation (Silver Layer)
├── hacker_news_normalizer/
│   └── handler.py                 # Lambda: Normalize HN Data
└── twitter_gold_stack/
    └── stack.py                   # Twitter Gold Metrics Setup
│
├── Analytics (Gold Layer)
├── twitter_gold_lambda/
│   └── handler.py                 # Lambda: Compute Twitter Metrics
├── hacker_news_transformator/
│   └── handler.py                 # Lambda: Compute HN Metrics
│
├── Database Loading
├── gold_postgres_loader_stack/
│   └── stack.py                   # PostgreSQL Loader Setup
└── gold_postgres_loader/
    └── handler.py                 # Lambda: Load Metrics to PostgreSQL
    └── psycopg2/                  # PostgreSQL Python Driver
│
└── Notifications
    └── discord_stack/
        └── handler.py             # Discord Alert Notifications
```

### Use Cases

1. **Real-time Trend Analysis**: Monitor trending topics on Hacker News and Bitcoin discussions on Twitter
2. **Community Analytics**: Analyze user engagement, karma trends, and participation patterns
3. **Data Quality Monitoring**: Track data completeness and quality metrics over time
4. **Business Intelligence**: Pre-aggregated metrics for dashboards and reports
5. **Historical Data Archive**: Complete, immutable record of historical data in Bronze layer

### Security Features

- **Encryption at Rest**: S3 bucket encryption enabled
- **Encryption in Transit**: VPC endpoints and secure networking
- **Access Control**: IAM roles with least privilege principle
- **Secrets Management**: Database credentials stored securely in AWS Secrets Manager
- **Network Isolation**: Private VPC with security groups
- **Data Retention**: S3 versioning enabled for data recovery
- **Audit Logging**: CloudWatch logs for all Lambda functions

### Monitoring & Observability

- **CloudWatch Metrics**: Performance metrics for Lambda, S3, and database operations
- **CloudWatch Logs**: Detailed logs from all data processing functions
- **CloudWatch Alarms**: Automated alerts for failures or performance degradation
- **SNS Integration**: Alert notifications to Discord and other services
- **Data Quality Scores**: Tracked metrics for data completeness and validation

### Deployment

The infrastructure is fully automated using AWS CDK:

```bash
# Install dependencies
pip install -r requirements.txt

# Deploy to AWS
cdk deploy --all

# Monitor deployment
cdk synth              
cdk destroy            
```

### Database Schema (Gold Layer)

The PostgreSQL database contains tables for each metric:

- `hn_daily_post_type_metric`: Daily Hacker News post counts by type
- `hn_top10_karma_highest`: Top 10 users by positive karma
- `hn_top10_karma_lowest`: Top 10 users by negative karma
- `hn_top10_jobs_by_score`: Trending job postings
- `hn_top10_stories_by_score`: Trending stories
- `hn_data_quality_score`: Data quality metrics for HN
- `twitter_daily_user_counts`: Daily unique user counts
- `twitter_top10_users_by_followers`: Most followed Bitcoin discussion participants
- `twitter_data_quality_score`: Data quality metrics for Twitter

### Data Quality & Validation

- HTML tag removal from text fields
- Timestamp parsing and normalization across multiple formats
- Duplicate user detection and consolidation
- Missing value handling with proper null tracking
- Data type validation and conversion
- Schema enforcement at each layer

### Notes

- Deployed to AWS region: **eu-north-1**
- All resources follow AWS best practices for security and cost optimization
- Infrastructure changes are tracked in version control for auditability
- Lambda functions use AWS SDK for Pandas layer for optimized data operations
- PostgreSQL database hosted on EC2 with security group restrictions

## Contributors

1. Bosko Vasilic
2. Marko Milutin
3. Sara Stojkov
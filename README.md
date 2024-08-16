# ToteSys - Data Engineering Project

# Summary
The project aims to implement a data platform that can extract data from an
operational database, archive it in a data lake, and make it easily accessible
within a remodelled OLAP data warehouse.

The solution showcases our skills in:

- Python
- PostgreSQL
- Database modelling
- Amazon Web Services (AWS)
- Agile methodologies

# Main Objective

Our goal is to create a reliable ETL (Extract, Transform, Load) pipeline that
can:

1. Extract the data from the `totesys` operational database
2. Store the data in AWS S3 buckets, that will form our data lake
3. Transform the data into a suitable schema for the data warehouse
4. Load the transformed data into the data warehouse hosted on AWS

# Key Features

We aim for the project to have certain features. Some are more prioritised than
others.

- [ ] Automated data ingestion from `totesys` db
- [ ] Data storage for ingested and processed data in S3 buckets
- [ ] Data transformation for data warehouse schema
- [ ] Automated data loading into the data warehouse schema
- [ ] Logging and monitoring with CloudWatch
- [ ] Notifications for errors and successful runs (e.g. successful ingestion)
- [ ] Visualisation of warehouse data

# Test Coverage
TBA

# Contributors
TBA
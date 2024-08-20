# ToteSys - Data Engineering Project

[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![AWS](https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)](https://www.terraform.io/)
[![Postgresql](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)](https://github.com/features/actions)

[![Terraform Main Deployment Workflow Status](https://img.shields.io/github/actions/workflow/status/ajschofield/de-project-bentley/deploy.yml?branch=main&style=flat-square&label=deploy)](https://github.com/ajschofield/de-project-bentley/actions/workflows/deploy.yml?query=branch%3Amain)
[![Production Environment Status](https://img.shields.io/github/deployments/ajschofield/de-project-bentley/production?style=flat-square&label=env)](https://github.com/ajschofield/de-project-bentley/deployments/production)
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

# Main Objectives

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

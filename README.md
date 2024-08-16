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
4. Load the data into the data warehouse hosted on AWS
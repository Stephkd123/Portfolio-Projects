# Portfolio-Projects

## ETL Creation and automation using Airflow 
This repository contains an ETL pipeline automation project designed to extract, transform, and load (ETL) movie rental data using Apache Airflow as the task orchestrator. The project processes data from the Sakila DB Schema and loads it into a destination data warehouse, ensuring structured data for analytics and business intelligence (BI) reporting.

# Project Overview 

Data Extraction: Data is pulled from the Sakila DB Schema, which includes both dimension and fact tables.
Data Loading: The extracted tables are created in the destination data warehouse under the stage and Sakila_DWh schemas, maintaining the original table structures.<br/>
Data Transformation: SQL transformation scripts are applied to enhance data quality for better querying and reporting.

## Data Model 
The following tables are included in the ETL pipeline:

- Dim_date
- Dim_time
- Dim_staff
- Dim_customer
- Dim_store
- Dim_film
- Dim_actor

Fact Table
- Fact_rental

## Data Transformations

To improve usability for BI analysts, the following transformations are applied:
- Standardizing movie ratings (e.g., PG, R, G, etc.).<br/>
- Formatting staff hire and termination dates for consistency.<br/>
- Updating store addresses to reflect the latest operational locations.

## Technology Stack
Apache Airflow – Task scheduling and workflow automation<br/>
MS SQL Server – Data storage and transformation<br/>
MsSQLCopyOperator – Handles database connections between two MS SQL databases

## Repository Contents
SQL Transform Files – Contains transformation logic for improved data quality.<br/>
Airflow DAGs – Defines the workflow for the ETL process.<br/>
Documentation – Detailed process flow and implementation notes.

For more details on the ETL process and implementation, refer to the repository files.

## ProductSales Analysis using SQL -- Tableau Visualization in Portfolio

- This a project on an Electronics company needing a database to handling sales transactions and track Agent performance across different locations around the world
- More description and SQl codes for this project are in the ProductSalesProject.sql file 




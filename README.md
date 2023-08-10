# Building an ETL pipeline using Apache Spark

## Introduction

While working with a team of Data Scientists on the task of identifying customers and invoices that are at risk of late payments. 

Write the ETL jobs to create the features (see below) that the scientists require for modelling.

The `data_extracts` folder contains csv extracts from 4 production tables:

- **accounts**
Contains general account information about customer accounts

- **skus**
Contains information about products or Stock Keeping Units (SKUs)

- **invoices**
Contains information about invoices issued to customers

- **invoice_line_items**
Contains detailed information about the products provided on each invoice

## Feature Engineering

Transform the following features from the data extracts :

- **inv_id**: the ID of the invoice to be passed into the model
- **acct_id**: the ID of the account for that invoice
- **inv_total**: Total value (in dollars) for the invoice
- **inv_items**: Total number of items in the invoice
- **acct_age**: The age of the account (in days) at the time the invoice was issued (i.e. the number of days between when the account was set up, and the invoice date)
- **num_inv_120d**: the number of invoices for the account in the 120 days prior to the invoice's issuing date
- **cum_tot_inv_acct**: The cumulative number of invoices for the account up to date that the invoice was issued.
- **is_late**: A flag to indicate if the invoice was paid late (i.e. more than 30 days after issuing). Contains `1` if invoice was paid late, `0` otherwise.

## Requirements

Create an ETL pipeline that:
- Produces a single dataset, with each row representing a single invoice, and the engineered features
- Uses Spark to transform the raw data into the features required by the scientists
- Tests the ETL job using the CSV extracts 
- Highlight any intermediate data models used to store the data
- Highlight any design choices, as well as scaling considerations


## Delivery

- Pipeline uses PySpark 3.4.0  
- Transformations using SparkSQL
- Run from repo root: ``` python Spark_ETL_Pipeline/src/Spark_ETL_Pipeline.py ```
- Final dataset `data_outputs/pipeline_output.csv`
- Potential scaling considerations including endpoints for extracting and loading data, data volumes, and ETL frequencies.

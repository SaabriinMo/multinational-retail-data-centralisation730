# Multinational Retail Data Centralisation

## Table of Contents
1. [Project Description](#project-description)
    - [Main Technologies Used](#main-technologies-used)
2. [Data Cleaning](#data-cleaning)
3. [Entity Relationship Diagram (ERD)](#entity-relationship-diagram-erd)
4. [Data Analysis](#data-analysis)
5. [Installation Instructions](#installation-instructions)
    - [For conda users](#for-conda-users)
    - [For pip users](#for-pip-users)
6. [Usage instructions](#usage-instructions)
7. [File Structure of the Project](#file-structure-of-the-project)
8. [License Information](#license-information)

## Project Description
Currently, the sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

This project aims to collect sales data from different sources, clean it and upload the new data into a postgresql database in order to perform some analysis. These data sources include using AWS s3 buckets, AWS RDS, and  APIs and etc.

The main motivation behind this project is to understand how data different sources are combined into one source.

This project's main functionalities are in the following files:

- data_cleaning.py: containing the DataCleaning class to clean the data
- data_extraction.py: containing the DataExtractor class to extract data from multiple sources
- database_utils.py: containing the DatabaseConnector to connect with postgres/AWS RDS

The file that simulates the Extract, Clean and Upload the  data to the local database is in:

- main.py: the main script containing the Extract, Clean and Upload logic to the database (i.e. PostgreSQL).


### Main technologies used

Python Library:
1. Data Cleaning: **Pandas, Numpy, and re**
2. Data extraction: **tabula, requests, boto3(AWS), botocore(AWS)**
3. Database utils: **sqlalchemy**, **yaml**

![aws](https://img.shields.io/badge/Amazon_AWS-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white) ![postgres](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white) ![python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)


## Data Cleaning

As mentioned above, the following table comes from different data sources:

1. User table and Order table: uses AWS RDS
2. cards table: PDF document in an AWS S3 bucket.
3. store table: using API
4. products table: CSV format in an S3 bucket on AWS.
5. dates event table: JSON format stored on S3 bucket on AWS


These tables consisted of many erroneous values, NULL values or errors with formatting. So the DataCleaning class was designed to clean these tables for it to be uploaded to my local Postgresql.


## Entity Relationship Diagram (ERD)
The ERD using the star schema using the orders table as the source of truth.

The other 4 table are linked to the orders table through Primary/Foreign key
![ERD](img/sales_database.png)

## Data Analysis
Queries were written to analyse and answer key questions on business performance.

You'll find the queries under [```analysis.sql```](queries/analysis.sql)


## Installation instructions
Git is used to manage and track the process of the project. If git is not installed check [here](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).

To clone this project:

```bash
git clone git@github.com:SaabriinMo/multinational-retail-data-centralisation730.git
```

If SSH is set up. HIGHLY recommend!

This project uses the conda environment

For a full list of the project's dependencies, check the enviroment.yml file in the project's root directory.

### For conda users
To create and activate the new enviroment:

```bash
conda env create -f environment.yml

 conda activate mrdc_env
```

### For pip users:
to be contiuned 

## Usage instructions

To run the custom ETL process, navigate to main.py file in your system and run this:

```bash 
python main.py
```

or 

```bash 
python3 main.py
```

## File structure of the project

```
.
├── README.md
├── creds
│   ├── api_creds.yaml
│   └── db_creds.yaml
├── data_cleaning.py
├── data_extraction.py
├── database_utils.py
├── environment.yml
├── img
│   └── sales_database.png
├── main.py
├── queries
│   ├── alterning_card_details_table.sql
│   ├── alterning_date_time_table.sql
│   ├── alterning_orders_table.sql
│   ├── alterning_products_table.sql
│   ├── alterning_store_table.sql
│   ├── alterning_users_table.sql
│   └── analysis.sql
└──

3 directories, 16 files
```

## License Information
..
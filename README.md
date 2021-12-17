# spotify-wrapped-etl

## Overview
A project that involved designing and implementing an ETL pipeline to provide a weekly Spotify summary listening, similarly to Spotify Wrapped.

## Extract
Spotify API endpoints were used to extract user listening history, 

## Transform
This data in turn was transformed using Python, which included data cleaning, and the engineering of useful features to provide unique identification and to lay the foundation for interesting summary metrics further down the pipeline.

## Load
The cleaned data was then store in temporary tables before being loaded into a local PostgreSQL database, which had been created using the appropriate SQL code to inlcude primary/foreign keys and data types. PgAdmin4 was used as a database management tool in parallel.  

![alt text](https://github.com/rob-field/spotify-wrapped-etl/blob/main/etl_schema%20(Small).PNG?raw=true)  


## Summary Email
SqlAlchemy was used to query the database (although SQL could have also been used), and functions were defined to return useful summary metrics. This information was then delivered by email to a specified user, also via Python.

## Automation
Apache Airflow DAGs were then defined to schedule to retrieval of the listening data from spotify, database loading and the sending of the weekly email at appropriate time points.

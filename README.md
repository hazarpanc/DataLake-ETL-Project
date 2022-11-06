# DataLake-ETL-Project
Portfolio project using AWS S3 as a data lake and running an ETL pipeline

## <br>Project Files

In addition to the data files, the project workspace includes 5 files:

**1. dl.cfg**                    Contains the Secret Key for ASW access<br>
**2. create_bucket.py**          Create bucket in AWS S3 to store the extracted dimentional tables.<br>
**3. etl.py**                    Loading song data and log data from S3 to Spark, transforms data into a set of dimensional tables, then save the table back to S3 <br>
**4. etl.ipynb**                 Used to design ETL pipelines <br>
**5. README.md**                 Provides project info<br>

# Project: Data Lake with Spark
This is a portfolio project I did part of Data Engineering nanodegree program. The goal of the project is to build an ETL pipeline that extracts data from S3, processes the data using Spark, and load the data back into another S3 bucket as a set of dimensional tables. The pipeline runs on an AWS EMR cluster with Spark.

#### Background
A music streaming startup, Sparkify, wants to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


## Data

The data for this project resides on Amazon S3.

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Sample data is available in the data folder in this repository.

#### Song Data

The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/).
Each file is in JSON format and contains metadata about a song and the artist of that song. 

The files are partitioned by the first three letters of each song's track ID. <br>
Example filepaths to files in this dataset are given below.

>**s3://udacity-dend/song_data/A/B/C/TRABCEI128F424C983.json**<br>
>**s3://udacity-dend/song_data/A/A/B/TRAABJL12903CDCF1A.json**

Structure of the song json file, **TRAABJL12903CDCF1A.json**, is shown below<br>
```
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud", 
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff", 
    "duration": 152.92036, <br>
    "year": 0    
}
```
#### Log Data

The second dataset consists of log files in JSON format. <br>
Files are partitioned by year and month. Example filepaths to files in this dataset are given below.

>**s3://udacity-dend/log_data/2018/11/2018-11-12-events.json**<br>
>**s3://udacity-dend/log_data/2018/11/2018-11-13-events.json**

The JSON files containing user activity have the following structure.


## How to Run
1. Enter the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the ```dwh.cfg``` configuration file.
2. Create an S3 bucket on AWS that will be the data lake. Change the output_data variable in the main() function with the bucket name.
3. Run ```etl.py``` on the cluster

## <br>Project Files

In addition to the data files, the project workspace includes 5 files:

**1. etl.py**                    Loading song data and log data from S3 to Spark, transforms data into a set of dimensional tables, then save the table back to S3 <br>
**2. Notebook.ipynb**            Contains the same code as in etl.py with the output the code generates <br>
**3. dl.cfg**                    Configuration file containing AWS IAM credentials
**4. README.md**                 Provides project information<br>

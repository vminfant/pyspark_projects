"""Script to transform the netflix data set from Kaggle.

Usage example:
    spark-submit netflix/netflix_pyspark.py -p data/netflix/
"""
import argparse
from pyspark.sql import SparkSession

# Define a schema for netflix data.
TITLES_SCHEMA = ("show_id STRING,type STRING,title STRING,director STRING,cast STRING,"
                 "country STRING,date_added DATE,release_year INT,rating STRING,"
                 "duration STRING,listed_in STRING,description STRING")

def process_df(spark, inp_df):
    """Process the input data frame and apply transformation rules."""
    return inp_df

def main():
    """Transform the netflix data set."""
    # Get arguments.
    parser = argparse.ArgumentParser(description="Transform netflix data")
    parser.add_argument("-p", "--path", type=str, required=True, help="Provide valid path")
    args = parser.parse_args()

    # Create a spark session.
    spark = SparkSession.builder.master("local").appName("netflix").getOrCreate()
    print(spark)

    # Load the data as a DataFrame.
    # Some of the description and titles are produced in multiple lines.
    inp_df = spark.read.csv(args.path, schema=TITLES_SCHEMA, header=True, multiLine=True)

    # Process the data frame
    process_df(spark, inp_df)

if __name__ == "__main__":
    main()

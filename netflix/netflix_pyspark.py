"""Script to transform the netflix data set from Kaggle."""
from pyspark.sql import SparkSession

DATA_PATH = "/home/jovyan/Git/pyspark_projects/data/netflix/"

def main():
    """Transform the netflix data set."""
    
    # Create a spark session.
    spark = SparkSession.builder.master("local").appName("netflix").getOrCreate()
    print(spark)

    # Define a schema for netflix data.
    titles_schema = ("show_id STRING,type STRING,title STRING,director STRING,cast STRING,"
                     "country STRING,date_added DATE,release_year INT,rating STRING,"
                     "duration STRING,listed_in STRING,description STRING")

    # Load the data as a DataFrame.
    inp_df = spark.read.csv(DATA_PATH, schema=titles_schema, header=True)

    print(inp_df.count())

if __name__ == "__main__":
    main()
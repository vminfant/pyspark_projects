"""Script to transform the netflix data set from Kaggle."""
from pyspark.sql import SparkSession

def main():
    """Transform the netflix data set."""
    
    # Create a spark session.
    spark = SparkSession.builder.master("local").appName("netflix").getOrCreate()
    print(spark)

    # Load the data as a DataFrame.
    inp_df = spark.read.

if __name__ == "__main__":
    main()
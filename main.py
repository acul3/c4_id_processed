from clean import clean_text
from pyspark.sql.types import StringType

FILE = "c4-nl.tfrecord-00000-of-01024.json.gz"
PATH ="/home/yeb/Developer/data/nrc_crawl_20210223"

def main():
    from pyspark.sql import SparkSession

    # logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder
            .config("spark.driver.memory", "15g")
            .getOrCreate()
    )
    # spark.sparkContext.setLogLevel(args.spark_log_level)

    sc = spark.sparkContext
    spark.udf.register("clean_text", clean_text, StringType())

    df = spark.read.json(f"{PATH}/{FILE}").repartition(12).limit(100)
    # df = spark.read.json(f"{PATH}/c4-nl.tfrecord-0000*").repartition(12).limit(100)

    df.createOrReplaceTempView("c4_table")
    clean_df = spark.sql("""
    SELECT url, timestamp, text, clean_text(text) as cleaned_text, abs(hash(url) % 100) as h
    FROM c4_table
    """).filter("cleaned_text is not null")
    clean_df.show()

    clean_df.write.partitionBy("h").format("json").save("/tmp/test")


if __name__ == '__main__':
    main()


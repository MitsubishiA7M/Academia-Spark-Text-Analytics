from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
import sys


class Project2:
    def run(self, inputPath, outputPath, stopwords, topic, k):
        spark = SparkSession.builder \
            .master("local") \
            .appName("project2_df") \
            .getOrCreate()

        n, m, topk = map(int, (stopwords, topic, k))

        df = spark.read.text(inputPath).toDF("line") \
            .withColumn("year", substring(col("line"), 1, 4)) \
            .withColumn("text", split(col("line"), ",").getItem(1)) \
            .withColumn("term", explode(split(col("text"), " ")))

        term_freq = df.groupBy("term").count()
        stopword_list = [
            row.term
            for row in term_freq
                .orderBy(desc("count"), asc("term"))
                .limit(n)
                .collect()
        ]

        df_filtered = df \
            .filter(~col("term").isin(stopword_list)) \
            .dropDuplicates(["year", "line", "term"])

        yearly_freq = df_filtered.groupBy("year", "term").count()

        windowSpec = Window.partitionBy("year") \
            .orderBy(desc("count"), asc("term"))

        ranked = yearly_freq \
            .withColumn("rank", row_number().over(windowSpec)) \
            .filter(col("rank") <= m)

        topk_df = ranked \
            .filter(col("rank") <= topk) \
            .orderBy("year", "rank")

        agg = topk_df.groupBy("year") \
            .agg(
                collect_list(concat_ws(" ", col("term"), col("count"))).alias("terms"),
                count("term").alias("total_terms")
            ) \
            .orderBy("year")

        result_rdd = agg.rdd.flatMap(
            lambda x: [f"{x['year']}:{x['total_terms']}"] + x['terms']
        )

        lines = result_rdd.collect()

        if outputPath.startswith("file://"):
            localOutput = outputPath[len("file://"):]
        else:
            localOutput = outputPath

        out_dir = os.path.dirname(localOutput)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir)

        with open(localOutput, 'w') as f:
            f.write("\n".join(lines))


        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(*sys.argv[1:])

from pyspark import SparkContext, SparkConf
import sys


class Project2:
    def run(self, inputPath, outputPath, stopwords, topic, k):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)

        n = int(stopwords)
        m = int(topic)
        topk = int(k)

        lines = sc.textFile(inputPath)
        year_terms = lines.map(lambda line: (
            line[:4],
            line.split(",", 1)[1].split()
        ))

        global_freq = year_terms \
            .flatMap(lambda x: x[1]) \
            .map(lambda term: (term, 1)) \
            .reduceByKey(lambda a, b: a + b)

        stopword_list = global_freq \
            .sortBy(lambda x: (-x[1], x[0])) \
            .map(lambda x: x[0]) \
            .take(n)
        stopword_set = set(stopword_list)

        filtered = year_terms.map(lambda x: (
            x[0],
            [t for t in x[1] if t not in stopword_set]
        ))

        yearly_freq = filtered \
            .flatMap(lambda x: [((x[0], term), 1) for term in x[1]]) \
            .reduceByKey(lambda a, b: a + b)

        by_year = yearly_freq \
            .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
            .groupByKey() \
            .mapValues(lambda lst: sorted(lst, key=lambda y: (-y[1], y[0]))[:m])
        topk_per_year = by_year.mapValues(lambda lst: lst[:topk]).sortByKey()

        formatted = topk_per_year.flatMap(
            lambda x: [f"{x[0]}:{len(x[1])}"] +
                      [f"{term} {count}" for term, count in x[1]]
        )

        lines_to_write = formatted.collect()

        if outputPath.startswith("file://"):
            localOutput = outputPath[len("file://"):]
        else:
            localOutput = outputPath

        with open(localOutput, 'w') as f:
            f.write("\n".join(lines_to_write))


        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(*sys.argv[1:])

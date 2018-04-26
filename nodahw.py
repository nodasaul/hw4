from pyspark import SparkContext

if __name__ == '__main__':
    sc = SparkContext()

    rdd = sc.textFile('/data/share/bdm/nyc_restaurants.csv', use_unicode=False).cache()
    # rest = sc.textFile('nyc_restaurants.csv', use_unicode=False).cache()

    def extract(partId, lines):
        if partId == 0:
            next(lines)
        import csv
        for row in csv.reader(lines):
            yield (row[0], (row[7], 1))

    rdd.mapPartitionsWithIndex(extract) \
        .reduceByKey(lambda x, y: x) \
        .values() \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: -x[1]) \
        .take(25)

    count = restaurants.reduceByKey(lambda x, y: x + y).saveAsTextFile("output")

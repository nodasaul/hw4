

from pyspark import SparkContext

if __name__ == '__main__':
	sc = SparkContext()

	rest = sc.textFile('/data/share/bdm/nyc_restaurants.csv', use_unicode=False).cache()
	# rest = sc.textFile('nyc_restaurants.csv', use_unicode=False).cache()


	def get_count(partId, records):
	    if partId==0:
	        records.next()
	        import csv
	        reader = csv.reader(records)
	        for row in reader:
	            (description)=(row[7])
	            yield (description,1)
	restaurants = rest.mapPartitionsWithIndex(get_count)
	restaurants.take(5)

	count = restaurants.reduceByKey(lambda x,y: x+y).saveAsTextFile("output")




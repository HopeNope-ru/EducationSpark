from pyspark import SparkContext

sc = SparkContext('local', 'RDD Read file')

rdd = sc.textFile('../content/test.csv')

rdd_lines = rdd.map(lambda line: line.split(','))

rdd_filtered = rdd_lines.filter(lambda col: col[1] == 'Dede')

rdd_filtered.saveAsTextFile('../content/rdd')

sc.stop()
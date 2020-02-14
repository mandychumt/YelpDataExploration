from pyspark import SparkContext
import json
import sys
sc = SparkContext()

input_file = sys.argv[1]
output_file = sys.argv[2]
partition_type = sys.argv[3]
n_partitions = int(sys.argv[4])
n = int(sys.argv[5])

# a function for obtaining n_items
def get_length (i):
	yield sum(1 for _ in i)

# Default Partition Function
def default(input_file, n):
	# create a RDD
	reviews = sc.textFile(input_file) \
				.map(lambda line: json.loads(line))

	# get business_id and corresponding reviews count
	reviews_count = reviews.map(lambda line: [line['business_id'], 1]) \
					.reduceByKey(lambda x, y: x + y)

	# get number of partitions
	n_partitions = reviews_count.getNumPartitions()

	# get number of items in each partition
	n_items = reviews_count.mapPartitions(get_length, True).collect()

	# get businesses that have more than n reviews
	result = reviews_count.filter(lambda line: line[1] > n).collect()

	return (n_items, result, n_partitions)

def customized(input_file, n_partitions, n):

	# create a RDD for business_id and corresponding reviews count
	# and customize the number of partitions and partitioner
	reviews_count = sc.textFile(input_file) \
					.map(lambda line: json.loads(line)) \
					.map(lambda line: [line['business_id'], 1]) \
					.partitionBy(n_partitions, lambda k: hash(ord(k[5]))) \
					.reduceByKey(lambda x, y: x + y)

    # get number of items in each partition
	n_items = reviews_count.mapPartitions(get_length, True).collect()

	# get businesses that have more than n reviews
	result = reviews_count.filter(lambda line: line[1] > n).collect()

	return (n_items, result, n_partitions)

if partition_type == 'default':
	results = default(input_file, n)
if partition_type == 'customized':
	results = customized(input_file, n_partitions, n)

ans = {"n_partitions": results[2], 'n_items': results[0], 'result': results[1]}
output = open(output_file, 'w')
json.dump(ans, output)
output.close()
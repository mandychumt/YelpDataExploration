from pyspark import SparkContext
import json
import sys
sc = SparkContext()

review_file = sys.argv[1]
business_file = sys.argv[2]
output_file = sys.argv[3]
if_spark = sys.argv[4]
n = int(sys.argv[5])


def spark(review_file, business_file, n):
	# create a RDD for business file
	business = sc.textFile(business_file).map(lambda line: json.loads(line))

	# create a RDD for review file
	reviews = sc.textFile(review_file).map(lambda line: json.loads(line))

	business_id_stars = reviews.map(lambda line: (line['business_id'], line['stars']))
	business_id_cat = business.map(lambda line: (line['business_id'], line['categories'])) \
	                .filter(lambda line: line[1] != None)

	def matchCatStar(t):
	    result = []
	    for cat in t[1][0].split(', '):
	        result.append([cat, t[1][1]])
	    return result

	result = business_id_cat.join(business_id_stars) \
	        .flatMap(matchCatStar) \
	        .aggregateByKey((0, 0), \
	                        lambda x, y: (x[0] + y, x[1] + 1), \
	                        lambda x1, y1: (x1[0] + y1[0], x1[1] + y1[1])) \
	        .map(lambda line: [line[0], line[1][0] / line[1][1]]) \
	        .sortBy(lambda x: (-x[1],x[0])) \
	        .take(n)

	return result


def no_spark(review_file, business_file, n):

	reviews_ls = []
	for line in open(review_file, 'r'):
	    reviews_ls.append(json.loads(line))

	business_ls = []
	for line in open(business_file, 'r'):
	    business_ls.append(json.loads(line))

	business_d = {} # key = business_id, value = cayegories
	for business in business_ls:
	    if business['categories'] != None:
	        id = business['business_id']
	        if id not in business_d:
	            business_d[id] = []
	        business_d[id].extend(business['categories'].split(', '))

	idAndStars = {} #key = business_id, value = list of starts
	for review in reviews_ls:
	    id_inReviewFile = review['business_id']
	    if id_inReviewFile in business_d:
	        if id_inReviewFile not in idAndStars:
	            idAndStars[id_inReviewFile] = []
	        idAndStars[id_inReviewFile].append(review['stars'])

	catAndStars = {} #key = categories, value = list of stars
	for k,v in idAndStars.items():
	    stars_ls = v
	    cat_ls = business_d[k]
	    for cat in cat_ls:
	        if cat not in catAndStars:
	            catAndStars[cat] = []
	        catAndStars[cat].extend(stars_ls)

	for k,v in catAndStars.items():
	    catAndStars[k] = sum(v) / len(v)
	result = list(catAndStars.items())
	result.sort(key = lambda k: (-k[1], k[0]), reverse = False)

	return result[0:n]


if if_spark == 'spark':
	result = spark(review_file, business_file, n)
if if_spark == 'no_spark':
	result = no_spark(review_file, business_file, n)

# output file
ans = {"result": result}
output = open(output_file, 'w')
json.dump(ans, output)
output.close()


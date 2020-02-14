from pyspark import SparkContext
import json
import sys
sc = SparkContext()

input_file = sys.argv[1]
output_file = sys.argv[2]
stopwords_file = sys.argv[3]
y = sys.argv[4]
m = int(sys.argv[5])
n = int(sys.argv[6])

# load data from input file and create a RDD called reviews
reviews = sc.textFile(input_file).map(lambda line: json.loads(line))

# A. total number of reviews
A = reviews.count() 

# B. number of reviews in a given year, y
B = reviews.filter(lambda line: line['date'] \
    .startswith(y)) \
    .count() 

# C. number of distinct users who have written reviews
C = reviews.map(lambda line: line['user_id']) \
    .distinct() \
    .count() 

# D. Top m users who have the largest number of reviews & its count
D = reviews.map(lambda line: (line['user_id'], 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: (-x[1],x[0])) \
    .map(lambda x: list(x)) \
    .take(m)

# E. Top n frequent words
punctuations = ['(', '[', ',', '.', '!', '?', ':', ';', ']', ')']
stopwords = []
for stopword in open(stopwords_file):
    stopwords.append(stopword[:-1])
def removePunc(word):
    for punc in punctuations:
        word = word.replace(punc, '')
    return word
E = reviews.flatMap(lambda line: line['text'].lower().split(' ')) \
    .filter(lambda word: word not in stopwords) \
    .map(removePunc) \
    .filter(lambda word: word != '') \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: (-x[1],x[0])) \
    .map(lambda x: x[0]) \
    .take(n)

# output file
ans = {"A": A, "B": B, "C": C, "D": D, "E": E}
output = open(output_file, 'w')
json.dump(ans, output)
output.close()
# YelpDataExploration

Tow datasets (review_small.json and business_small.json) are samples extracted from the Yelp dataset (https://www.yelp.com/dataset).

Sample review data:

{'review_id': '-I5umRTkhw15RqpKMl_o1Q',
  'user_id': '-mA3-1mN4JIEkqOtdbNXCQ',
  'business_id': 'mRUVMJkUGxrByzMQ2MuOpA',
  'stars': 1.0,
  'text': "Walked in around 4 on a Friday afternoon, we sat at a table just off the bar and walked out after 5 min or so. Don't even think they realized we walked in. However everyone at the bar noticed we walked in!!! Service was non existent at best. Not a good way for a new business to start out. Oh well, the location they are at has been about 5 different things over the past several years, so they will just be added to the list. SMDH!!!",
  'date': '2017-12-15 23:27:08'}

Sample business data:

{'business_id': '1SWheh84yJXfytovILXOAQ',
  'name': 'Arizona Biltmore Golf Club',
  'address': '2818 E Camino Acequia Drive',
  'city': 'Phoenix',
  'state': 'AZ',
  'postal_code': '85016',
  'latitude': 33.5221425,
  'longitude': -112.0184807,
  'stars': 3.0,
  'review_count': 5,
  'is_open': 0,
  'attributes': {'GoodForKids': 'False'},
  'categories': 'Golf, Active Life',
  'hours': None}

----------------------------------------------------------------------

TASK 1

Explore the review datasets:

A. The total number of reviews

B. The number of reviews in a given year, y

C. The number of distinct users who have written the reviews

D. Top m users who have the largest number of reviews and its count

E. Top n frequent words (lowercase) in the review text. The following punctuations i.e., “(”, “[”, “,”, “.”, “!”, “?”, “:”, “;”, “]”, “)”, and the given stopwords are excluded


Execution commands:

spark-submit task1.py <input_file> <output_file> <stopwords> <y> <m> <n>

input_file – the input file (the review dataset)

output_file – the output file contains answers

stopwords – the file contains the stopwords that will be removed for E

----------------------------------------------------------------------

TASK 2

Compute the average stars for each business category and output top n categories with the highest average stars (with an without spark).

Execution commands:

spark-submit task2.py <review_file> <business_file > <output_file> <if_spark> <n>

review _file – the input file (the review dataset)

business_file – the input file (the business dataset)

output_file – the output file contains answers

if_spark – either “spark” or “no_spark”

n – top n categories with highest average stars

----------------------------------------------------------------------

TASK 3

Compute the businesses that have more than n reviews in the review file. At the same time, show the number of partitions for the RDD and the number of items per partition with either default or customized partition function. The customized one will have higher computational efficiency.

Execution commands:

spark-submit task3.py <input_file> <output_file> <partition_type> <n_partitions> <n>

input_file – the input file (the review dataset)

output_file – the output file contains answers

partition_type – the partition function, either “default” or “customized”

n_partitions – the number of partitions (only effective for the customized partition function)

n – the threshold of the number of reviews

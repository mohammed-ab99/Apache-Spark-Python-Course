# regex to normalize the text and remove , . etc...
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("dataset/book.txt")
words = input.flatMap(normalizeWords)

# count words
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# sort words ASC
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)

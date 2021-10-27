from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("SpentByCustomer")
sc = SparkContext(conf=conf)

# read data
input = sc.textFile('dataset/customer-orders.csv')


def extractCustomerPricePairs(line):
    # split by comma
    fields = line.split(',')
    # return and cast fields to corresponding data type
    return (int(fields[0]), float(fields[2]))


# apply the extraction
mappedInput = input.map(extractCustomerPricePairs)
# find the total
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

# flip the <k, v> to <v, k>
flipped = totalByCustomer.map(lambda x: (x[1], x[0]))
# sort
totalByCustomerSorted = flipped.sortByKey()

# print
results = totalByCustomer.collect()
for result in results:
    print(result)

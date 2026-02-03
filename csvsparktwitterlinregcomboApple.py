from __future__ import print_function

from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import *


from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover
from pyspark.ml.feature import StringIndexer, IDF
from pyspark.ml import Pipeline

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import RegressionEvaluator

# Create a SparkSession (Note, the config section is only for Windows!)
#spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///temp").appName("LinearRegression").getOrCreate()
spark = SparkSession \
    .builder \
    .appName("Twitter Sentiment Analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
# Load up our data and convert it to the format MLLib expects.
#inputLines = spark.sparkContext.textFile("regression.txt")
#data = inputLines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))
with open('strmoutput.txt', 'r') as reader:
    with open('cleanedoutput.csv', 'w') as writer:
        for line in reader:
            if line[0]=="0":
                writer.write(line)


trainingDataframe = spark.read.csv("training2.csv", sep=',', multiLine=True, header = False)
tweets_csv = spark.read.csv("Apple-Twitter-Sentiment-DFE.csv",sep=',', multiLine=True, header = False)
testdataframe = spark.read.csv("testdata.csv", sep=',', multiLine=True, header = False)
tweetstreamcsv=spark.read.csv("cleanedoutput.csv", sep='||', multiLine=True, header = False)


#separate out the 2 cols

datatrain = trainingDataframe.select("_c0", "_c5")
testData = testdataframe.select("_c0", "_c5")
appledata=tweets_csv.select("_c5","_c12")
streamdata=tweetstreamcsv.select("_c0", "_c5")


# Convert this RDD to a DataFrame
colNames = ["humanlabel", "tweet"]
df = datatrain.toDF("humanlabel","tweet")
dftest=testData.toDF("humanlabel","tweet")
dfapple=appledata.toDF("humanlabel","tweet")
dfstream=streamdata.toDF("humanlabel","tweet")

# Note, there are lots of cases where you can avoid going from an RDD to a DataFrame.
# Perhaps you're importing data from a real database. Or you are using structured streaming
# to get your data.

resultdf=dfapple.union(dftest)
resultdf=resultdf.union(df)
df=resultdf


# Let's split our data into training data and testing data
#trainTest = df.randomSplit([0.95, 0.05])
trainingDF = df
testDF = dfstream

#resultdf=trainingDF.union(dftest)
#trainingDF=resultdf

#pipeline

tokenizer = Tokenizer(inputCol="tweet",outputCol="words")
hashtf = HashingTF(numFeatures=2**16, inputCol="words", outputCol='tf')
idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5) #minDocFreq: remove sparse terms
label_stringIdx = StringIndexer(inputCol = "humanlabel", outputCol = "label")
#,handleInvalid="keep")
lr = LogisticRegression(maxIter=100,family="multinomial")
pipeline = Pipeline(stages=[tokenizer, hashtf, idf, label_stringIdx,lr])

model = pipeline.fit(trainingDF)
#print(model.stages[4])
#print(model.stages[3])
labs=model.stages[3].labels
#print(model.stages[4].summary)

# Now create our linear regression model
#lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
# Train the model using our training data
#model = lir.fit(trainingDF)

# Now see if we can predict values in our test data.
# Generate predictions using our linear regression model for all features in our
# test dataframe: testDF or dftest
fullPredictions = model.transform(testDF).cache()
#fullPredictions.show(1)
# Extract the predictions and the "known" correct labels.
predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
labels = fullPredictions.select("tweet").rdd.map(lambda x: x[0])
probability = fullPredictions.select("probability").rdd.map(lambda x: x[0])
#print(probability.take(10))
# Zip them together
predictionAndLabel = predictions.zip(labels).collect()
probzip = probability.zip(labels).collect()
#print(probzip[1])


# Print out the predicted and actual values for each point
#print(len(predictionAndLabel))
i=0
num0s=0
num1s=0
num2s=0
for prediction in predictionAndLabel:
    predictstr=''.join(str(prediction))
    prob=probzip[i][0]
    if abs(prob[0]-prob[1])<.2:
       predictstr=predictstr.replace("0.0","2.0").replace("1.0","2.0")
    if i%1==0:
        newstr=predictstr.replace("0.0",labs[0]).replace("1.0",labs[1]).replace("2.0",labs[2])
        print(newstr[1:-1])
    if "0.0," in predictstr:
        num0s=num0s+1
    elif "1.0," in predictstr:
        num1s=num1s+1
    elif "2.0," in predictstr:
        num2s=num2s+1

    i=i+1
#print("Prediction, Label")
ones=fullPredictions.filter(1 == fullPredictions.prediction).count()
twos=fullPredictions.filter(2 == fullPredictions.prediction).count()
zeros=fullPredictions.filter(0 == fullPredictions.prediction).count()
totals=fullPredictions.count()
#accuracy = fullPredictions.filter(fullPredictions.label == fullPredictions.prediction).count() / fl>
#print("total tweets: = %g" % totals)
#print("total %g: = %g" % (int(labs[0]), zeros))
#print("total %g: = %g" % (int(labs[2]), twos))
#print("total %g: = %g" % (int(labs[1]), ones))

#print("after fixing")
#print("label", labs[0], "count", str(num0s))
#print("label", labs[1], "count", str(num1s))
#print("label", labs[2], "count", str(num2s))


# Stop the session
spark.stop()




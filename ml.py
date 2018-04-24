from pyspark import SparkContext, SparkConf

from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import functions as f
from pyspark.sql.functions import col, split
from pyspark.sql.types import Row, DoubleType

from pyspark.ml import Pipeline

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LinearSVC, LogisticRegression
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover, Word2Vec

from pyspark.ml.linalg import Vectors, SparseVector, DenseVector

import argparse
import ast

NUM_PER_DAY = 50


def create_dict(x):
    d = {}
    for i in x:
        if i == 'created_utc' or i == 'body' or i == 'score' or i == 'controversiality':
            d[i] = str(x[i])
    return d

def combine_text(rows):
    d = {}
    time = None
    text_array = []
    text = ""
    for row in rows:
        text_array.append((int(row['score']), row['body']))
        time = row['created_utc']
    text_array.sort(key=lambda tup: tup[0], reverse=True)
    num = min(NUM_PER_DAY,len(text_array))
    for i in range(num):
        #text += text_array[i][1]
        text += " " + parseAndRemoveStopWords(text_array[i][1])
    d['created_utc'] = time
    d['body'] = text

    return d


def parseAndRemoveStopWords(text):
    t = text.replace(";"," ").replace(":"," ").replace('"',' ').replace('-',' ').replace("?"," ")
    t = t.replace(',',' ').replace('.',' ').replace('!','').replace("/"," ").replace("\\"," ")
    t = t.replace('@',' ').replace('&',' ').replace('#',' ').replace('*',' ').replace('%',' ')
    #t = t.lower().split(" ")
    t = t.lower()
    return t


def read_reddit(sc, sqlContext, filenames):

    input = sc.textFile(filenames)
    json_file = input.map(ast.literal_eval)
    data = json_file.map(lambda x: Row(**create_dict(x)))
    data = data.map(lambda x: (x['created_utc'],x))

    #data = data.filter(lambda x: int(x[1]['score'] > 0))
    data = data.filter(lambda x: int(x[1]['body'] != "[deleted]"))
    data = data.filter(lambda x: int(x[1]['body'] != "[removed]"))
    data = data.filter(lambda x: int(x[1]['controversiality']) <= 0)

    data = data.groupByKey().mapValues(lambda x: Row(**combine_text(x)))
    df   = data.map(lambda x: x[1]).toDF()
    return df


def read_stock(sqlContext, filename):

    df = sqlContext.read.csv(filename, header=True)
    return df


def combine(sqlContext, reddit_df, stock_df):

    df = reddit_df.join(stock_df, reddit_df.created_utc == stock_df.Date)
    df.drop('Symbol').drop('created_utc')
    return df

def get_label(df):

    df = df.withColumn("Close",df["Close"].cast(DoubleType()))
    df = df.withColumn("Open",df["Open"].cast(DoubleType()))
    df = df.withColumn("label", df["Close"] > df["Open"])
    df = df.withColumn("label",df["label"].cast(DoubleType()))
    df = df.select("label","body", "Date")
    return df


def non_random_split(df):

    #clean the stopword
    df = clean_stopword(df)
    # input: label body Date
    # output: label words Date

    #count total row number and where to split
    rowNum = df.count()
    splitIndex = int(rowNum * 0.8)

    #zip with index, output: label, words, Date, index
    dfRDD = df.rdd.sortBy(lambda x: x[2]).zipWithIndex()
    #dfRDD = df.rdd

    #split col
    newDF = dfRDD.map(lambda x: (x[0][0], x[0][1], x[0][2], x[1])).toDF(["label", "words", "Date", "index"])
    IndexLabel = newDF.select("index", "label", "Date")  # "index", "label"
    IndexWords = newDF.select("index", "words")  # "index", "words"

    #set delay
    IndexLabel = IndexLabel.rdd.map(lambda x: (x[0] + 1, x[1])).toDF(["index", "label", "Date"])
    IndexWords = IndexWords.rdd.map(lambda x: (x[0] - 1, x[1])).toDF(["index", "words"])

    #indexlabel drop last 2, indexwords drop first 2
    IndexLabel = IndexLabel.rdd.filter(lambda x: x[0]< rowNum-2).toDF(["index", "label", "Date"])
    IndexWords = IndexWords.rdd.filter(lambda x: x[0]> 2).toDF(["index", "words"])

    #output: index label words
    IndexWords = IndexWords.select(col("words"), col("index").alias("index2"))
    df = IndexLabel.join(IndexWords, IndexLabel.index == IndexWords.index2)
    df = df.select(col("Date"), col("index"),col("label"),col("words"))

    training = df.rdd.filter(lambda x: x[0] < splitIndex).toDF(["Date", "index", "label", "words"])
    test = df.rdd.filter(lambda x: x[0] > splitIndex).toDF(["Date", "index", "label", "words"])

    print("\n\n\n\nTraining")
    training.show()
    print("\n\n\n\nTest")
    test.show()

    return training, test #format: "Date", "index", "label", "words"


def clean_stopword(df):
    print("\n\n\n\n origional body")
    df.select("body").show()
    df = df.select(col("label"), split(col("body"), " \s*").alias("body"), col("Date"))

    print("\n\n\n\n split body")
    df.select("body").show()
    b = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at",
         "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did", "do",
         "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having",
         "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how",
         "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "it", "it's", "its", "itself",
         "let's", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought",
         "our", "ours", "ourselves", "out", "over", "own", "same", "she", "she'd", "she'll", "she's", "should", "so",
         "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
         "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to",
         "too", "under", "until", "up", "very", "was", "we", "we'd", "we'll", "we're", "we've", "were", "what",
         "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's",
         "with", "would", "you", "you'd", "you'll", " ", "you're", "you've", "your", "yours", "yourself", "yourselves"]
    remover = StopWordsRemover(inputCol="body", outputCol="words", stopWords=b)
    df = remover.transform(df)

    print("\n\n\n\n After stopWords")
    df.select("words").show()

    return df #label words Date

def train_svm_idf(sqlContext, df):

    training, test = df.randomSplit([0.8, 0.2])

    tokenizer = Tokenizer(inputCol="body", outputCol="words")

    hashingTF = HashingTF(numFeatures=2000,
                          inputCol=tokenizer.getOutputCol(),
                          outputCol="rawFeatures")

    idf = IDF(inputCol=hashingTF.getOutputCol(),outputCol="features")
    svm = LinearSVC(featuresCol="features",labelCol="label")

    pipline = Pipeline(stages=[tokenizer, hashingTF, idf, svm])
    model   = pipline.fit(training)

    test_df = model.transform(test)
    train_df  = model.transform(training)

    test_df.show()
    train_df.show()

    evaluator=BinaryClassificationEvaluator(labelCol="label")
    """rawPredictionCol="prediction","""

    train_metrix = evaluator.evaluate(train_df)
    test_metrix = evaluator.evaluate(test_df)
    test_p = test_df.select("prediction").rdd.map(lambda x:x['prediction']).collect()
    test_l = test_df.select("label").rdd.map(lambda x:x['label']).collect()
    train_p = train_df.select("prediction").rdd.map(lambda x:x['prediction']).collect()
    train_l = train_df.select("label").rdd.map(lambda x:x['label']).collect()

    print("\n\n\n\n")
    print("-" * 15 + " OUTPUT " + "-" * 15)
    print()
    print("confusion matrix for trainning data")
    print(train_metrix)
    print("train label")
    print(train_l)
    print("train prediction")
    print(train_p)
    print("-" * 30)
    print()
    print("confusion matrix for testing data")
    print(test_metrix)
    print("test label")
    print(test_l)
    print("test prediction")
    print(test_p)

    print("-" * 30)
    print("\n\n\n\n")

def train_svm_word2vec(sqlContext, df):
   
    #input: "label", "body", "Date"
    training, test = non_random_split(df)
    #training, test = df.randomSplit([0.8, 0.2])
 
    df = clean_stopword(df) #label words Date
    print("df input:")
    df.show()

    word2Vec = Word2Vec(vectorSize=100, minCount=10,
                        inputCol="words", outputCol="word2vec")

    modelW2V = word2Vec.fit(df)
    print("\n\n\n\n word2vec GetVector")
    modelW2V.getVectors().show()

    # _temp_a = modelW2V.transform(training).select("word2vec").rdd.map(lambda x:x["word2vec"]).take(1)
    # _temp_b = modelW2V.getVectors().select("vector").rdd.map(lambda x:x["vector"]).take(1)

    trainDF = modelW2V.transform(training)
    trainDF = trainDF.select(col("label").alias("label"), col("word2vec").alias("features"))
    testDF = modelW2V.transform(test)
    testDF = testDF.select(col("label").alias("label"), col("word2vec").alias("features"))
    logistic = LogisticRegression(regParam=0.01, labelCol="label",  featuresCol="features")

    model = logistic.fit(trainDF)

    train_df = model.transform(trainDF)
    test_df = model.transform(testDF)

    print("model.transform(trainDF):")
    train_df.show()
    #labelCol = result.select("label")
    #predictCol = result.select("prediction")

    #test_df.select("label").show()
    #test_df.select("prediction").show()

    evaluator=BinaryClassificationEvaluator(labelCol="label")
    """rawPredictionCol="prediction","""

    train_metrix = evaluator.evaluate(train_df)
    test_metrix = evaluator.evaluate(test_df)
    test_p = test_df.select("prediction").rdd.map(lambda x:x['prediction']).collect()
    test_l = test_df.select("label").rdd.map(lambda x:x['label']).collect()
    train_p = train_df.select("prediction").rdd.map(lambda x:x['prediction']).collect()
    train_l = train_df.select("label").rdd.map(lambda x:x['label']).collect()

    print("\n\n\n\n")
    print("-" * 15 + " OUTPUT " + "-" * 15)
    print()
    print("confusion matrix for trainning data")
    print(train_metrix)
    print("train label")
    print(train_l)
    print("train prediction")
    print(train_p)
    print("-" * 30)
    print()
    print("confusion matrix for testing data")
    print(test_metrix)
    print("test label")
    print(test_l)
    print("test prediction")
    print(test_p)

    print("-" * 30)
    print("\n\n\n\n")


if __name__ == '__main__':
    #Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('reddit')
    parser.add_argument('stock')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("subreddit_stock_prediction")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # read data into dataframe
    reddit_df = read_reddit(sc, sqlContext, args.reddit)
    stock_df = read_stock(sqlContext, args.stock)

    # combine
    df = combine(sqlContext, reddit_df, stock_df)

    # create label
    df = get_label(df)

    # train model
    train_svm_word2vec(sqlContext, df)

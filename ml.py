from pyspark import SparkContext, SparkConf

from pyspark.sql import SQLContext
from pyspark.sql.types import Row, DoubleType 
from pyspark.sql import functions as f
from pyspark.sql.functions import col, split
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Word2Vec
#from /nltk_data/corpora import stopwords

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
        text += text_array[i][1]
        #text += parseAndRemoveStopWords(text_array[i][1])
    d['created_utc'] = time
    d['body'] = text
    
    return d


def parseAndRemoveStopWords(text):
    t = text[0].replace(";"," ").replace(":"," ").replace('"',' ').replace('-',' ').replace("?"," ")
    t = t.replace(',',' ').replace('.',' ').replace('!','').replace("'"," ").replace("/"," ").replace("\\"," ")
    t = t.lower().split(" ")
    stop = stopwords.words('english')
    return [i for i in t if i not in stop]


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
    df = df.select("label","body")
    return df

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
    #redditRDD = df.select('body').rdd
    #reRDD = redditRDD.map(parseAndRemoveStopWords)
    #twDF = reRDD.map(lambda p: Row(body=p)).toDF()

    df = df.select(col("label"), split(col("body"), " \s*").alias("body"))
    training, test = df.randomSplit([0.8, 0.2])

    #print(count = df.count())
    
    #tokenizer = Tokenizer(inputCol="body", outputCol="words")

    word2Vec = Word2Vec(vectorSize=100, minCount=10,
                        inputCol="body", outputCol="word2vec")

    modelW2V = word2Vec.fit(df)
    modelW2V.getVectors().show()

    test.map(lambda )

    #svm = LinearSVC(featuresCol="word2vec",labelCol="label")
    #pipline = Pipeline(stages=[tokenizer, word2Vec, svm])

    #model   = pipline.fit(training)

    #test_df = model.transform(test)
    #train_df  = model.transform(training)

    #test_df.show()
    #train_df.show()


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

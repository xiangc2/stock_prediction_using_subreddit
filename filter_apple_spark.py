from pyspark import SparkContext, SparkConf
import argparse
import json
import re

def filter_out(x):
    return x["subreddit"] == "apple"


def filter_apple(sc, reddit_filename):
    input = sc.textFile(reddit_filename)
    json_file = input.map(json.loads)
    output = json_file.filter(filter_out)

    return output









if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Reddit data from')
    parser.add_argument('output', help='File to save RDD to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("filter_apple_out")
    sc = SparkContext(conf=conf)

    results = filter_apple(sc, args.input)
    results.coalesce(1).saveAsTextFile(args.output)

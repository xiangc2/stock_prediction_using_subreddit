from pyspark import SparkContext, SparkConf
import argparse
import re
from datetime import date
import ast

def filter_time(x):
    try:
        x["created_utc"] = date.fromtimestamp(int(x["created_utc"])).isoformat()
    except:
        return x["created_utc"]
    return x


def filter_apple(sc, reddit_filename):
    input = sc.textFile(reddit_filename)
    json_file = input.map(ast.literal_eval)
    output = json_file.map(filter_time)

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
    results.saveAsTextFile(args.output)

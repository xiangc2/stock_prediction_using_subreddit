# Stock Trend Prediction with Reddit Posts

## Abstraction:

Accurately predicting stock price is a hot research topic among trading companies. Being able to predict stock price accurately not only helps company make money, but also helps to supervise the market and avoid potential economic crisis. Most of the prediction today are based on professional data such as portfolios. Our research tried to do the prediction based on public voice instead of these financial indicators. We pick AAPL (stock of apple) as our prediction target, and subreddit post as the feature data we used to extract people’s opinion. different feature extraction methods like TF-IDF and Word2vec are performed on the feature data. The result feature vectors together with the stock status are feed into machine learning algorithm to train a logistic model . We tried out different tricks in feature extraction and gradually enhance our model. At the end, all improved model achieves an accuracy better than 50%, with highest accuracy 56.84%. The noise is very big, and is hard for us to say our model works well in predicting the stock price. However, the experiments did show some useful method that could be used to improve the accuracy of predicting stock price.

## Dataset:

Google Finance AAPL stock data from 1/1/2014 to 11/30/2017

All subreddit post from 1/1/2014 to 11/30/2017

## Platform:

AWS

## Framework:

Spark

## package:

pyspark

## Files:

1. download.py

    Collect and create download_links.

2. download_links.txt

    Links of needed data set.

3. filter_apple_spark.py

    Filter data by subreddit.

4. filter_time.py

    Convert timestamp to humanized date.

5. ml.py

    Core machine learning algorithm script.

6. prepare

    Bash script for automatically uploading, filtering, and writing log.

## Usage:

### Data collect

1. Obtain download-links

   ```bash
   # This will create a download_links.txt.
   # The two parameters are time span, start month & end-month
   python download.py 2014-01 2017-11
   ```

2. Download

   ```bash
   # On the cluster
   wget -i -c download.txt
   ```

3. Extract

   ```bash
   # This will cost about 20 minutes for each file.
   bzip2 -d RC_2014-01.bz2 # -d will delete the original archive
   ```

4. Upload to hdfs

   ```bash
   hdfs dfs -copyFromLocal RC_2014-01 hdfs:///projects/group5/
   ```

5. Filter

   ```bash
   # Filter by subreddit
   spark-submit --master yarn --deploy-mode client filter_apple_spark.py hdfs:///projects/group5/RC_2014-01 hdfs:///projects/group5/2014-01
       
   # Convert the time (The input depends on your own directory)
   spark-submit --master yarn --deploy-mode client filter_time.py hdfs:///projects/group5/201*/part* hdfs:///projects/group5/time_filtered
   ```
       
### Machine Learning
        
```bash
spark-submit --master yarn --deploy-mode client ml.py [subreddit_file] [stock_data_file]
# Example:
spark-submit --master yarn --deploy-mode client ml.py hdfs:///projects/group5/filter_time/part-00043 hdfs:///projects/group5/stock
```

**Improvement(auto bash script)**

​	This script will help automatically upload files to hdfs, filter data, and leave log.

​	Use your favorite editor changing the last few lines of the script, which indicates what files you are working on.

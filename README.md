# Stock Trend Prediction with Reddit Posts

## Introduction:

Earlier studies on stock market prediction based on public voice use Twitter Data or New Articles as the source. News always reflect what have happened on the stock market, therefore,  not an ideal data source for predicting the future. In addition , because of the unpredictability in News and Twitter moments stock market prices follow a random walk pattern and cannot be predicted with more than 50% accuracy[1]. Unlike the previous study. we pick subreddit for apple and apple’s product as our source. They are more targeted community, and the comment are more sentimental rather than just stating the fact. So it serves well as the source to extract public opinion about the company. what’s more, most of the previous studies label each post as positive and negative by hand. This restrict the size of their source data, and its high cost of labor makes the prediction process not applicable to the industrial situation. In our research, we tried on merging all the post in one day,  and run an unsupervised model on the merged text to automatically extract the sentiment feature.  Though this may result in lower accuracy. **It is now possible to train model on a much larger dataset and  makes the prediction process more automatic.(make more sense in real situation in industry)** We also performed some original tricks on feature extraction, like set post from the previous day and label from the current day together as the learning data, and include the label from the previous day as the current day’s feature.

## Dataset:

Google Finance AAPL stock data from 1/1/2014 to 11/30/2017

All subreddit post from 1/1/2014 to 11/30/2017

## Environment:

Course Cluster

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
       spark-submit --master yarn --deploy-mode client filter_apple_spark.py hdfs:///projects/group5/RC_2014-01 hdfs:///projects/group5/2014-01
       ```
       
### Machine Learning
        
       
        spark-submit --master yarn --deploy-mode client ml.py [subreddit_file] [stock_data_file]
        
    example
        
        

**Improvement(auto bash script)**

​	This script will help automatically upload files to hdfs, filter data, and leave log.

​	Use your favorite editor changing the last few lines of the script, which indicates what files you are working on.
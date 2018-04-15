# Download and filter reddit data

1. upload download.py to your cluster repo.

2. run download.py

   ```bash
   python download.py 2014-01 2017-12 # start date & end date
   ```

3. download data

   ```bash
   wget -c -i download_links.txt 
   # run this command in tmux since it will cost a lot of time
   ```

4. extract data

   ```bash
   bzip2 -d RC_2014-01.bz2 # -d will delete the original archive
   ```

5. copy to hdfs

   ```bash
   hdfs dfs -copyFromLocal RC_2014-01 hdfs:///projects/group5/

   # after copy to hdfs, delete the original file.
   rm RC_2014-01
   ```

6. filter data

   ```bash
   spark-submit --master yarn --deploy-mode client filter_apple_spark.py hdfs:///projects/group5/RC_2014-01 hdfs:///projects/group5/2014-01

   # after filter data, delete the original file
   hdfs dfs -rm RC_2014-01
   ```

   ​
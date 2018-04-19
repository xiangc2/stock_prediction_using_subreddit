# UPDATE: 2018-04-18

Time format has been updated. timestamp --> string eg: 2017-11-02

spark can have multiple input

```bash
# Input multi-files. Sure
spark-submit --master yarn --deploy-mode client filter_time.py hdfs:///projects/group5/201*/part* hdfs:///projects/group5/filter_time

# Input a directory. Not sure about ./success file.
spark-submit --master yarn --deploy-mode client filter_time.py hdfs:///projects/group5/201*/ hdfs:///projects/group5/filter_time

# In .py
sc.textfile("file1", "file2", ...)
```



Output file from spark will have single quoted string instead of double quoted string. However, json syntax can only recognize double quoted string.

**single-quoted:** 'created_utc': '2017-08-31'

**double-quoted:** "created_utc": "2017-08-31"

```python
import ast

f = open(file)
line = f.readline()
structured = ast.literal_eval(line)

# filter_time.py
json_file = lines.map(ast.literal_eval)
```


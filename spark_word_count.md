**complete step-by-step Spark Word Count using PySpark + HDFS**.

## 1. Login using PuTTY

Login to master machine:

```bash
student@spark-master
```

or using IP:

```bash
student@192.168.5.18
```

## 2. Switch to spark user

```bash
su - spark
```

Enter spark user password.

## 3. Create project folder

```bash
mkdir -p ~/cse-be/808/word_count
cd ~/cse-be/808/word_count
```

## 4. Create input file locally

```bash
nano input.txt
```

Paste:

```text
hello spark
hello big data
spark cluster word count
big data analytics using spark
```

Save:

```text
CTRL + O
Enter
CTRL + X
```

## 5. Create HDFS input directory

```bash
hdfs dfs -mkdir -p /spark/cse-be/808/word_count/input
```

## 6. Upload input file to HDFS

```bash
hdfs dfs -put -f input.txt /spark/cse-be/808/word_count/input/
```

## 7. Verify HDFS input

```bash
hdfs dfs -ls /spark/cse-be/808/word_count/input
hdfs dfs -cat /spark/cse-be/808/word_count/input/input.txt
```

## 8. Create PySpark program

```bash
nano wordcount.py
```

Paste this code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WordCount") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

sc = spark.sparkContext

input_path = "hdfs://spark-master:9000/spark/cse-be/808/word_count/input/input.txt"
output_path = "hdfs://spark-master:9000/spark/cse-be/808/word_count/output"

text = sc.textFile(input_path)

counts = (
    text.flatMap(lambda line: line.split())
        .map(lambda word: (word.lower(), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
)

counts.saveAsTextFile(output_path)

spark.stop()
```

Save and exit.

## 9. Delete old output if exists

Spark will fail if output folder already exists.

```bash
hdfs dfs -rm -r /spark/cse-be/808/word_count/output
```

If it says no such file, ignore it.

## 10. Run Spark Word Count job

```bash
/opt/spark/spark-4.1.1-bin-hadoop3/bin/spark-submit \
--master spark://spark-master:7077 \
wordcount.py
```

## 11. Check output in HDFS

```bash
hdfs dfs -ls /spark/cse-be/808/word_count/output
```

You should see:

```text
_SUCCESS
part-00000
part-00001
```

## 12. Display result

```bash
hdfs dfs -cat /spark/cse-be/808/word_count/output/part-*
```

Expected output:

```text
(analytics, 1)
(big, 2)
(cluster, 1)
(count, 1)
(data, 2)
(hello, 2)
(spark, 3)
(using, 1)
(word, 1)
```

## 13. Check in browser

HDFS browser:

```text
http://spark-master:9870/explorer.html#/spark/cse-be/808/word_count/output
```

Spark UI:

```text
http://spark-master:8080
```

Done: **input from HDFS → Spark cluster processing → output stored in HDFS**.

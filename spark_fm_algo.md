**FM Algorithm / Flajolet-Martin Algorithm** step-by-step practical process using PySpark.

## Aim

To implement the Flajolet-Martin algorithm using Apache Spark to estimate the number of distinct elements in a dataset.

## Concept

Flajolet-Martin algorithm is used to estimate **unique/distinct count** in large datasets.

Example use cases:

```text
Unique visitors
Unique IP addresses
Unique users
Distinct words
```

It is useful when data is very large and exact counting requires more memory.

---

## Step 1: Login using PuTTY

```bash
student@spark-master
```

or:

```bash
student@192.168.5.18
```

---

## Step 2: Switch to spark user

```bash
su - spark
```

---

## Step 3: Create project folder

```bash
mkdir -p ~/cse-be/808/fm_algorithm
cd ~/cse-be/808/fm_algorithm
```

---

## Step 4: Create input file locally

```bash
nano visitors.txt
```

Paste this:

```text
user1
user2
user3
user1
user4
user2
user5
user6
user7
user8
user9
user10
user3
user11
user12
```

Save:

```text
CTRL + O
Enter
CTRL + X
```

---

## Step 5: Create HDFS input folder

```bash
hdfs dfs -mkdir -p /spark/cse-be/808/fm_algorithm/input
```

---

## Step 6: Upload input file to HDFS

```bash
hdfs dfs -put -f visitors.txt /spark/cse-be/808/fm_algorithm/input/
```

---

## Step 7: Verify HDFS input

```bash
hdfs dfs -ls /spark/cse-be/808/fm_algorithm/input
hdfs dfs -cat /spark/cse-be/808/fm_algorithm/input/visitors.txt
```

---

## Step 8: Create PySpark program

```bash
nano fm_algorithm.py
```

Paste this code:

```python
from pyspark.sql import SparkSession
import hashlib
import math

spark = SparkSession.builder \
    .appName("FlajoletMartinAlgorithm") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

sc = spark.sparkContext

input_path = "hdfs://spark-master:9000/spark/cse-be/808/fm_algorithm/input/visitors.txt"
output_path = "hdfs://spark-master:9000/spark/cse-be/808/fm_algorithm/output"

def hash_value(item):
    return int(hashlib.md5(item.encode()).hexdigest(), 16)

def trailing_zeros(num):
    if num == 0:
        return 0

    count = 0
    while (num & 1) == 0:
        count += 1
        num = num >> 1

    return count

data = sc.textFile(input_path)

unique_items = data.map(lambda x: x.strip()).filter(lambda x: x != "")

max_trailing_zeros = unique_items.map(
    lambda item: trailing_zeros(hash_value(item))
).max()

estimated_distinct_count = 2 ** max_trailing_zeros

exact_distinct_count = unique_items.distinct().count()

result = [
    "Flajolet-Martin Algorithm Result",
    "----------------------------------",
    f"Maximum trailing zeros found : {max_trailing_zeros}",
    f"Estimated distinct count     : {estimated_distinct_count}",
    f"Exact distinct count         : {exact_distinct_count}"
]

result_rdd = sc.parallelize(result)

result_rdd.coalesce(1).saveAsTextFile(output_path)

spark.stop()
```

---

## Step 9: Delete old output if exists

```bash
hdfs dfs -rm -r /spark/cse-be/808/fm_algorithm/output
```

Ignore the error if the folder does not exist.

---

## Step 10: Run Spark program

```bash
/opt/spark/spark-4.1.1-bin-hadoop3/bin/spark-submit \
--master spark://spark-master:7077 \
fm_algorithm.py
```

---

## Step 11: Check HDFS output

```bash
hdfs dfs -ls /spark/cse-be/808/fm_algorithm/output
```

Expected:

```text
_SUCCESS
part-00000
```

---

## Step 12: View output

```bash
hdfs dfs -cat /spark/cse-be/808/fm_algorithm/output/part-*
```

Sample output:

```text
Flajolet-Martin Algorithm Result
----------------------------------
Maximum trailing zeros found : 4
Estimated distinct count     : 16
Exact distinct count         : 12
```

---

## Step 13: View in browser

```text
http://spark-master:9870/explorer.html#/spark/cse-be/808/fm_algorithm/output
```

Open:

```text
part-00000
```

---

## Result

The Flajolet-Martin algorithm was successfully implemented using Apache Spark. The approximate number of distinct users was calculated and stored in HDFS as a single output file.

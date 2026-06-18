Below is the **Bloom Filter Algorithm step-by-step practical process** using PySpark.

## Aim

To implement a Bloom Filter using Apache Spark to check whether a given item may exist in a dataset.

## Concept

Bloom Filter is a space-efficient probabilistic data structure.

It gives:

```text
Present / May be Present
Definitely Not Present
```

It may give false positive, but never false negative.

---

## Step 1: Login using PuTTY

```bash
student@spark-master
```

or

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
mkdir -p ~/cse-be/808/bloom_filter
cd ~/cse-be/808/bloom_filter
```

---

## Step 4: Create Python file

```bash
nano bloom_filter.py
```

---

## Step 5: Paste this code

```python
from pyspark.sql import SparkSession
import hashlib

spark = SparkSession.builder \
    .appName("BloomFilterAlgorithm") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

sc = spark.sparkContext

# Bloom Filter size
SIZE = 20

# Create bit array
bit_array = [0] * SIZE

# Sample dataset
items = [
    "apple",
    "banana",
    "mango",
    "orange",
    "grapes"
]

# Hash function 1
def hash1(item):
    return int(hashlib.md5(item.encode()).hexdigest(), 16) % SIZE

# Hash function 2
def hash2(item):
    return int(hashlib.sha1(item.encode()).hexdigest(), 16) % SIZE

# Insert item into Bloom Filter
def insert(item):
    index1 = hash1(item)
    index2 = hash2(item)

    bit_array[index1] = 1
    bit_array[index2] = 1

# Check item in Bloom Filter
def check(item):
    index1 = hash1(item)
    index2 = hash2(item)

    if bit_array[index1] == 1 and bit_array[index2] == 1:
        return f"{item} may be present"
    else:
        return f"{item} is definitely not present"

# Insert all items
for item in items:
    insert(item)

# Items to search
search_items = [
    "apple",
    "banana",
    "cherry",
    "watermelon",
    "mango"
]

# Parallelize search using Spark
search_rdd = sc.parallelize(search_items)

result_rdd = search_rdd.map(lambda item: check(item))

# Store output in HDFS as single file
output_path = "hdfs://spark-master:9000/spark/cse-be/808/bloom_filter/output"

result_rdd.coalesce(1).saveAsTextFile(output_path)

spark.stop()
```

Save:

```text
CTRL + O
Enter
CTRL + X
```

---

## Step 6: Delete old output if exists

```bash
hdfs dfs -rm -r /spark/cse-be/808/bloom_filter/output
```

Ignore error if path does not exist.

---

## Step 7: Run Spark program

```bash
/opt/spark/spark-4.1.1-bin-hadoop3/bin/spark-submit \
--master spark://spark-master:7077 \
bloom_filter.py
```

---

## Step 8: Check HDFS output

```bash
hdfs dfs -ls /spark/cse-be/808/bloom_filter/output
```

Expected:

```text
_SUCCESS
part-00000
```

---

## Step 9: View output

```bash
hdfs dfs -cat /spark/cse-be/808/bloom_filter/output/part-*
```

Expected output:

```text
apple may be present
banana may be present
cherry is definitely not present
watermelon is definitely not present
mango may be present
```

---

## Step 10: View in browser

```text
http://spark-master:9870/explorer.html#/spark/cse-be/808/bloom_filter/output
```

Open:

```text
part-00000
```

Done.

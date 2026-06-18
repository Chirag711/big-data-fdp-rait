**DGIM Algorithm step-by-step practical process using PySpark**.

## Aim

To implement the DGIM algorithm using Apache Spark to estimate the number of `1`s in the last `N` bits of a binary data stream.

## Concept

DGIM stands for **Datar-Gionis-Indyk-Motwani** algorithm.

It is used for counting `1`s in a sliding window over a binary stream using less memory.

Example:

```text
Stream: 1 0 1 1 0 1 0 1 1 1
Window size: last 8 bits
Task: Estimate number of 1s in last 8 bits
```

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
mkdir -p ~/cse-be/808/dgim_algorithm
cd ~/cse-be/808/dgim_algorithm
```

---

## Step 4: Create input file

```bash
nano binary_stream.txt
```

Paste:

```text
1
0
1
1
0
1
0
1
1
1
0
0
1
1
0
1
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
hdfs dfs -mkdir -p /spark/cse-be/808/dgim_algorithm/input
```

---

## Step 6: Upload input file to HDFS

```bash
hdfs dfs -put -f binary_stream.txt /spark/cse-be/808/dgim_algorithm/input/
```

---

## Step 7: Verify HDFS input

```bash
hdfs dfs -ls /spark/cse-be/808/dgim_algorithm/input
hdfs dfs -cat /spark/cse-be/808/dgim_algorithm/input/binary_stream.txt
```

---

## Step 8: Create PySpark program

```bash
nano dgim_algorithm.py
```

Paste this code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DGIMAlgorithm") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

sc = spark.sparkContext

input_path = "hdfs://spark-master:9000/spark/cse-be/808/dgim_algorithm/input/binary_stream.txt"
output_path = "hdfs://spark-master:9000/spark/cse-be/808/dgim_algorithm/output"

# Window size
WINDOW_SIZE = 8

data = sc.textFile(input_path)

bits = data.map(lambda x: int(x.strip())) \
           .filter(lambda x: x == 0 or x == 1) \
           .collect()

buckets = []

# Each bucket is stored as:
# [size, timestamp]

def remove_old_buckets(current_time):
    global buckets
    buckets = [
        bucket for bucket in buckets
        if current_time - bucket[1] < WINDOW_SIZE
    ]

def merge_buckets():
    global buckets

    size_count = {}

    for bucket in buckets[:]:
        size = bucket[0]
        if size not in size_count:
            size_count[size] = []
        size_count[size].append(bucket)

        if len(size_count[size]) > 2:
            oldest1 = size_count[size][0]
            oldest2 = size_count[size][1]

            new_bucket = [
                size * 2,
                oldest2[1]
            ]

            buckets.remove(oldest1)
            buckets.remove(oldest2)
            buckets.append(new_bucket)

            buckets.sort(key=lambda x: x[1], reverse=True)
            merge_buckets()
            return

def process_stream(bits):
    time = 0

    for bit in bits:
        time += 1

        if bit == 1:
            buckets.insert(0, [1, time])
            merge_buckets()

        remove_old_buckets(time)

    return time

def estimate_count():
    if len(buckets) == 0:
        return 0

    total = 0

    for i in range(len(buckets)):
        if i == len(buckets) - 1:
            total += buckets[i][0] // 2
        else:
            total += buckets[i][0]

    return total

current_time = process_stream(bits)

actual_count = sum(bits[-WINDOW_SIZE:])
estimated_count = estimate_count()

result = [
    "DGIM Algorithm Result",
    "---------------------",
    f"Binary Stream              : {bits}",
    f"Window Size                : {WINDOW_SIZE}",
    f"Last {WINDOW_SIZE} Bits     : {bits[-WINDOW_SIZE:]}",
    f"Actual Count of 1s         : {actual_count}",
    f"Estimated Count of 1s      : {estimated_count}",
    f"Buckets [size, timestamp]  : {buckets}"
]

result_rdd = sc.parallelize(result)

# Create only one output file
result_rdd.coalesce(1).saveAsTextFile(output_path)

spark.stop()
```

---

## Step 9: Delete old output if exists

```bash
hdfs dfs -rm -r /spark/cse-be/808/dgim_algorithm/output
```

Ignore error if output does not exist.

---

## Step 10: Run Spark job

```bash
/opt/spark/spark-4.1.1-bin-hadoop3/bin/spark-submit \
--master spark://spark-master:7077 \
dgim_algorithm.py
```

---

## Step 11: Check HDFS output

```bash
hdfs dfs -ls /spark/cse-be/808/dgim_algorithm/output
```

Expected:

```text
_SUCCESS
part-00000
```

---

## Step 12: View output

```bash
hdfs dfs -cat /spark/cse-be/808/dgim_algorithm/output/part-*
```

Sample output:

```text
DGIM Algorithm Result
---------------------
Binary Stream              : [1, 0, 1, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 0, 1]
Window Size                : 8
Last 8 Bits                : [1, 1, 0, 0, 1, 1, 0, 1]
Actual Count of 1s         : 5
Estimated Count of 1s      : 5
Buckets [size, timestamp]  : [[1, 16], [2, 14], [2, 10]]
```

---

## Step 13: View in HDFS Browser

Open:

```text
http://spark-master:9870/explorer.html#/spark/cse-be/808/dgim_algorithm/output
```

Click:

```text
part-00000
```

---

## Step 14: Check Spark UI

Open:

```text
http://spark-master:8080
```

Application name:

```text
DGIMAlgorithm
```

## Result

The DGIM algorithm was successfully implemented using PySpark. It estimated the number of `1`s in the last `N` bits of a binary stream, and the output was stored in HDFS as a single output file.

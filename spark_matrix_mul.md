# EXPERIMENT NO. 2

## Title

Matrix Multiplication (2×3 × 3×2) Using Apache Spark and HDFS

---

## Aim

To perform matrix multiplication using Apache Spark on a Spark Cluster and store the output in HDFS.

---

## Objective

1. Read matrix data in Spark.
2. Perform matrix multiplication using Python for loops.
3. Execute the application on a Spark Cluster.
4. Store the result in HDFS.
5. Verify output using HDFS commands and Web UI.

---

## Software Requirements

* Ubuntu Linux
* Apache Hadoop
* Apache Spark 4.1.1
* Python 3.x
* HDFS
* PuTTY

---

## Cluster Configuration

| Component          | Details                  |
| ------------------ | ------------------------ |
| Master Node        | spark-master             |
| Worker Nodes       | spark-w-01 to spark-w-15 |
| Spark Master Port  | 7077                     |
| Spark UI Port      | 8080                     |
| HDFS NameNode Port | 9000                     |
| HDFS Web UI        | 9870                     |

---

## Theory

Matrix multiplication is performed when the number of columns in Matrix A equals the number of rows in Matrix B.

Matrix A (2×3)

1 2 3

4 5 6

Matrix B (3×2)

7  8

9 10

11 12

Result Matrix C (2×2)

58   64

139 154

Formula:

C[i][j] = Σ(A[i][k] × B[k][j])

---

## Algorithm

1. Create Spark Session.
2. Define Matrix A and Matrix B.
3. Create an empty result matrix.
4. Use three nested loops:

   * Outer loop for rows of Matrix A
   * Middle loop for columns of Matrix B
   * Inner loop for multiplication and summation
5. Store results in an RDD.
6. Save RDD output into HDFS.
7. Verify output.

---

## Procedure

### Step 1: Login to Master Node

```bash
student@spark-master
```

### Step 2: Switch to Spark User

```bash
su - spark
```

### Step 3: Create Working Directory

```bash
mkdir -p ~/cse-be/808/matrix_multiplication
cd ~/cse-be/808/matrix_multiplication
```

### Step 4: Create Python Program

```bash
nano matrix_multiplication.py
```

Paste the following code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MatrixMultiplication") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

sc = spark.sparkContext

A = [
    [1, 2, 3],
    [4, 5, 6]
]

B = [
    [7, 8],
    [9, 10],
    [11, 12]
]

rows_A = len(A)
cols_A = len(A[0])

rows_B = len(B)
cols_B = len(B[0])

C = [[0 for j in range(cols_B)] for i in range(rows_A)]

for i in range(rows_A):
    for j in range(cols_B):
        for k in range(cols_A):
            C[i][j] += A[i][k] * B[k][j]

result_rdd = sc.parallelize(
    [f"C[{i}][{j}] = {C[i][j]}"
     for i in range(rows_A)
     for j in range(cols_B)]
)

result_rdd.coalesce(1).saveAsTextFile(
    "hdfs://spark-master:9000/spark/cse-be/808/matrix_multiplication/output"
)

spark.stop()
```

### Step 5: Delete Previous Output

```bash
hdfs dfs -rm -r /spark/cse-be/808/matrix_multiplication/output
```

### Step 6: Execute Program

```bash
spark-submit \
--master spark://spark-master:7077 \
matrix_multiplication.py
```

### Step 7: Verify Output

```bash
hdfs dfs -ls /spark/cse-be/808/matrix_multiplication/output
```

### Step 8: Display Result

```bash
hdfs dfs -cat /spark/cse-be/808/matrix_multiplication/output/part-*
```

---

## Output

```text
C[0][0] = 58
C[0][1] = 64
C[1][0] = 139
C[1][1] = 154
```

---

## HDFS Verification

List Output Files:

```bash
hdfs dfs -ls /spark/cse-be/808/matrix_multiplication/output
```

Expected:

```text
_SUCCESS
part-00000
```

---

## Web UI Verification

### HDFS Web UI

http://spark-master:9870

Navigate to:

```text
/spark/cse-be/808/matrix_multiplication/output
```

### Spark Web UI

http://spark-master:8080

Verify that the application "MatrixMultiplication" has executed successfully.

---

## Result

Matrix multiplication of a 2×3 matrix and a 3×2 matrix was successfully performed using Apache Spark. The result was stored in HDFS as a single output file using coalesce(1) and verified through both command-line utilities and web interfaces.

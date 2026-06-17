Using the same Hadoop Streaming setup, create a **3 × 2 matrix multiplication project**.

We will multiply:

```text
A = 3 × 2 matrix
B = 2 × 3 matrix
Result C = 3 × 3 matrix
```

Project path:

```bash
/home/hadoop/cse-be/808/matrix_3x2
```

---

## 1. Login to master using PuTTY

```bash
ssh hadoop@192.168.5.35
```

---

## 2. Create project folder

```bash
mkdir -p /home/hadoop/cse-be/808/matrix_3x2
cd /home/hadoop/cse-be/808/matrix_3x2
```

---

## 3. Matrix example

Matrix A:

```text
1 2
3 4
5 6
```

Matrix B:

```text
7 8 9
10 11 12
```

Result:

```text
C = A × B

C[0,0] = 1*7 + 2*10 = 27
C[0,1] = 1*8 + 2*11 = 30
C[0,2] = 1*9 + 2*12 = 33

C[1,0] = 3*7 + 4*10 = 61
C[1,1] = 3*8 + 4*11 = 68
C[1,2] = 3*9 + 4*12 = 75

C[2,0] = 5*7 + 6*10 = 95
C[2,1] = 5*8 + 6*11 = 106
C[2,2] = 5*9 + 6*12 = 117
```

---

## 4. Create input file

Format:

```text
A row col value
B row col value
```

```bash
cat > input.txt <<'EOF'
A 0 0 1
A 0 1 2
A 1 0 3
A 1 1 4
A 2 0 5
A 2 1 6
B 0 0 7
B 0 1 8
B 0 2 9
B 1 0 10
B 1 1 11
B 1 2 12
EOF
```

---

## 5. Create mapper.py

```bash
cat > mapper.py <<'EOF'
#!/usr/bin/env python3
import sys

# A is 3x2
# B is 2x3
# C will be 3x3

A_ROWS = 3
A_COLS = 2
B_COLS = 3

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    matrix, i, j, value = line.split()
    i = int(i)
    j = int(j)
    value = float(value)

    if matrix == "A":
        # A[i][j] is needed for all C[i][k]
        for k in range(B_COLS):
            print(f"{i},{k}\tA,{j},{value}")

    elif matrix == "B":
        # B[i][j] is needed for all C[k][j]
        for k in range(A_ROWS):
            print(f"{k},{j}\tB,{i},{value}")
EOF

chmod +x mapper.py
```

---

## 6. Create reducer.py

```bash
cat > reducer.py <<'EOF'
#!/usr/bin/env python3
import sys

current_key = None
a_values = {}
b_values = {}

def emit_result(key, a_values, b_values):
    total = 0

    for index in a_values:
        if index in b_values:
            total += a_values[index] * b_values[index]

    if total.is_integer():
        total = int(total)

    print(f"{key}\t{total}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    key, value = line.split("\t")
    matrix, index, number = value.split(",")

    index = int(index)
    number = float(number)

    if current_key is None:
        current_key = key

    if key != current_key:
        emit_result(current_key, a_values, b_values)
        current_key = key
        a_values = {}
        b_values = {}

    if matrix == "A":
        a_values[index] = number
    elif matrix == "B":
        b_values[index] = number

if current_key is not None:
    emit_result(current_key, a_values, b_values)
EOF

chmod +x reducer.py
```

---

## 7. Test locally

```bash
cat input.txt | python3 mapper.py | sort | python3 reducer.py
```

Expected output:

```text
0,0     27
0,1     30
0,2     33
1,0     61
1,1     68
1,2     75
2,0     95
2,1     106
2,2     117
```

---

## 8. Upload input to HDFS

```bash
hdfs dfs -mkdir -p /user/hadoop/cse-be/808/matrix_3x2/input
hdfs dfs -put -f input.txt /user/hadoop/cse-be/808/matrix_3x2/input/
```

---

## 9. Remove old output

```bash
hdfs dfs -rm -r -skipTrash /user/hadoop/cse-be/808/matrix_3x2/output
```

Ignore error if output does not exist.

---

## 10. Run Hadoop Streaming job

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py \
-input /user/hadoop/cse-be/808/matrix_3x2/input/input.txt \
-output /user/hadoop/cse-be/808/matrix_3x2/output \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py"
```

---

## 11. Check final output

```bash
hdfs dfs -cat /user/hadoop/cse-be/808/matrix_3x2/output/part-00000
```

---

## 12. Create driver file

```bash
cat > run.sh <<'EOF'
#!/bin/bash
set -e

BASE=/user/hadoop/cse-be/808/matrix_3x2
INPUT=$BASE/input
OUTPUT=$BASE/output

echo "Creating HDFS input directory..."
hdfs dfs -mkdir -p $INPUT

echo "Uploading input file..."
hdfs dfs -put -f input.txt $INPUT/

echo "Removing old output..."
hdfs dfs -rm -r -skipTrash $OUTPUT 2>/dev/null || true

echo "Running 3x2 Matrix Multiplication Hadoop Job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py \
-input $INPUT/input.txt \
-output $OUTPUT \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py"

echo "Final Matrix Output:"
hdfs dfs -cat $OUTPUT/part-00000
EOF

chmod +x run.sh
```

Run:

```bash
./run.sh
```

---

## 13. Web UI

NameNode UI:

```text
http://192.168.5.35:9870
```

YARN UI:

```text
http://192.168.5.35:8088
```

Output path:

```text
/user/hadoop/cse-be/808/matrix_3x2/output
```

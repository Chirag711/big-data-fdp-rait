Below is the complete **Bloom Filter Hadoop Streaming execution process** with code.

Project path:

```bash
/home/hadoop/cse-be/808/bloom_filter
```

---

## 1. Login to master using PuTTY

Host:

```text
192.168.5.35
```

Username:

```text
hadoop
```

---

## 2. Create project folder

```bash
mkdir -p /home/hadoop/cse-be/808/bloom_filter
cd /home/hadoop/cse-be/808/bloom_filter
```

---

## 3. Create input file: `numbers.txt`

These are inserted elements.

```bash
cat > numbers.txt <<'EOF'
8
10
EOF
```

---

## 4. Create query file: `query.txt`

These are elements to check.

```bash
cat > query.txt <<'EOF'
48
7
EOF
```

---

## 5. Create `mapper.py`

```bash
cat > mapper.py <<'EOF'
#!/usr/bin/env python3
import sys

def hash_functions(x):
    return [
        (3 * x + 3) % 6,
        (3 * x + 7) % 8,
        (2 * x + 9) % 2,
        (2 * x + 3) % 5
    ]

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    x = int(line)
    positions = hash_functions(x)

    for pos in positions:
        print(f"BIT\t{pos}")

    print(f"ELEMENT\t{x}")
EOF

chmod +x mapper.py
```

---

## 6. Create `reducer.py`

```bash
cat > reducer.py <<'EOF'
#!/usr/bin/env python3
import sys
import os

ARRAY_SIZE = 10
bloom_array = [0] * ARRAY_SIZE
inserted_elements = []

def hash_functions(x):
    return [
        (3 * x + 3) % 6,
        (3 * x + 7) % 8,
        (2 * x + 9) % 2,
        (2 * x + 3) % 5
    ]

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    key, value = line.split("\t")

    if key == "BIT":
        bloom_array[int(value)] = 1

    elif key == "ELEMENT":
        inserted_elements.append(int(value))

print("===== BLOOM FILTER RESULT =====")
print("Inserted Elements:", inserted_elements)
print("Bloom Filter Array:", bloom_array)
print()

if os.path.exists("query.txt"):
    with open("query.txt", "r") as f:
        queries = [int(x.strip()) for x in f if x.strip()]

    for q in queries:
        positions = hash_functions(q)
        bit_values = [bloom_array[p] for p in positions]

        if all(v == 1 for v in bit_values):
            result = "Probably Present"
        else:
            result = "Surely Not Present"

        print(f"Element: {q}")
        print(f"Hash Positions: {positions}")
        print(f"Bit Values: {bit_values}")
        print(f"Result: {result}")
        print()
else:
    print("query.txt not found")
EOF

chmod +x reducer.py
```

---

## 7. Local test

```bash
cat numbers.txt | python3 mapper.py | sort | python3 reducer.py
```

Expected:

```text
Element: 48
Result: Probably Present

Element: 7
Result: Surely Not Present
```

---

## 8. Create HDFS input directory

```bash
hdfs dfs -mkdir -p /user/hadoop/cse-be/808/bloom_filter/input
```

---

## 9. Upload input to HDFS

```bash
hdfs dfs -put -f numbers.txt /user/hadoop/cse-be/808/bloom_filter/input/
```

Verify:

```bash
hdfs dfs -ls /user/hadoop/cse-be/808/bloom_filter/input
```

---

## 10. Remove old output

```bash
hdfs dfs -rm -r -skipTrash /user/hadoop/cse-be/808/bloom_filter/output
```

Ignore error if output does not exist.

---

## 11. Run Hadoop Streaming job

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py,query.txt \
-input /user/hadoop/cse-be/808/bloom_filter/input/numbers.txt \
-output /user/hadoop/cse-be/808/bloom_filter/output \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py"
```

---

## 12. Check final output

```bash
hdfs dfs -cat /user/hadoop/cse-be/808/bloom_filter/output/part-00000
```

---

## 13. Create driver file `run.sh`

```bash
cat > run.sh <<'EOF'
#!/bin/bash
set -e

BASE=/user/hadoop/cse-be/808/bloom_filter
INPUT=$BASE/input
OUTPUT=$BASE/output

echo "===== BLOOM FILTER HADOOP PROJECT ====="

echo "[1] Creating HDFS input directory..."
hdfs dfs -mkdir -p $INPUT

echo "[2] Uploading numbers.txt to HDFS..."
hdfs dfs -put -f numbers.txt $INPUT/

echo "[3] Removing old output..."
hdfs dfs -rm -r -skipTrash $OUTPUT 2>/dev/null || true

echo "[4] Running Hadoop Streaming Bloom Filter job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py,query.txt \
-input $INPUT/numbers.txt \
-output $OUTPUT \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py"

echo "[5] Final Output:"
hdfs dfs -cat $OUTPUT/part-00000
EOF

chmod +x run.sh
```

Run:

```bash
./run.sh
```

---

## 14. Check in Web UI

NameNode UI:

```text
http://192.168.5.35:9870
```

Output path:

```text
/user/hadoop/cse-be/808/bloom_filter/output
```

YARN UI:

```text
http://192.168.5.35:8088
```

---

## Final Project Structure

```text
/home/hadoop/cse-be/808/bloom_filter
├── numbers.txt
├── query.txt
├── mapper.py
├── reducer.py
└── run.sh
```
---

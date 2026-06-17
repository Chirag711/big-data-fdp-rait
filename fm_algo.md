Below is the complete **FM Algorithm Hadoop Streaming execution process** with code.

FM stands for **Flajolet-Martin Algorithm**. It is used to estimate the **number of distinct elements** in a data stream using hashing and the position of the rightmost `1` bit.

Project path:

```bash
/home/hadoop/cse-be/808/fm_algo
```

---

# 1. Login to Master using PuTTY

Host:

```text
192.168.5.35
```

Username:

```text
hadoop
```

---

# 2. Create Project Folder

```bash
mkdir -p /home/hadoop/cse-be/808/fm_algo
cd /home/hadoop/cse-be/808/fm_algo
```

---

# 3. Create Input File

```bash
cat > stream.txt <<'EOF'
apple
banana
orange
grape
apple
mango
banana
kiwi
orange
watermelon
grape
apple
EOF
```

---

# 4. Create Mapper Code

```bash
cat > mapper.py <<'EOF'
#!/usr/bin/env python3
import sys
import hashlib

def trailing_zeros(n):
    if n == 0:
        return 32

    count = 0
    while (n & 1) == 0:
        count += 1
        n >>= 1

    return count

def hash_value(item):
    h = hashlib.md5(item.encode()).hexdigest()
    return int(h, 16)

for line in sys.stdin:
    item = line.strip()

    if not item:
        continue

    h = hash_value(item)
    rho = trailing_zeros(h)

    print(f"FM\t{rho}")
EOF

chmod +x mapper.py
```

---

# 5. Create Reducer Code

```bash
cat > reducer.py <<'EOF'
#!/usr/bin/env python3
import sys
import math

max_rho = 0
total_records = 0

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    key, value = line.split("\t")
    rho = int(value)

    total_records += 1

    if rho > max_rho:
        max_rho = rho

estimate = 2 ** max_rho

print("===== FM ALGORITHM RESULT =====")
print(f"Total Stream Records Processed: {total_records}")
print(f"Maximum Trailing Zero Count R: {max_rho}")
print(f"Estimated Number of Distinct Elements: {estimate}")
EOF

chmod +x reducer.py
```

---

# 6. Local Test

```bash
cat stream.txt | python3 mapper.py | sort | python3 reducer.py
```

---

# 7. Create HDFS Input Directory

```bash
hdfs dfs -mkdir -p /user/hadoop/cse-be/808/fm_algo/input
```

---

# 8. Upload Input File to HDFS

```bash
hdfs dfs -put -f stream.txt /user/hadoop/cse-be/808/fm_algo/input/
```

Verify:

```bash
hdfs dfs -ls /user/hadoop/cse-be/808/fm_algo/input
```

---

# 9. Remove Old Output Directory

```bash
hdfs dfs -rm -r -skipTrash /user/hadoop/cse-be/808/fm_algo/output
```

Ignore error if output does not exist.

---

# 10. Run Hadoop Streaming Job

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py \
-input /user/hadoop/cse-be/808/fm_algo/input/stream.txt \
-output /user/hadoop/cse-be/808/fm_algo/output \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py"
```

---

# 11. Check Output

```bash
hdfs dfs -cat /user/hadoop/cse-be/808/fm_algo/output/part-00000
```

---

# 12. Create Driver File

```bash
cat > run.sh <<'EOF'
#!/bin/bash
set -e

BASE=/user/hadoop/cse-be/808/fm_algo
INPUT=$BASE/input
OUTPUT=$BASE/output

echo "===== FM ALGORITHM HADOOP PROJECT ====="

echo "[1] Creating HDFS input directory..."
hdfs dfs -mkdir -p $INPUT

echo "[2] Uploading stream.txt..."
hdfs dfs -put -f stream.txt $INPUT/

echo "[3] Removing old output..."
hdfs dfs -rm -r -skipTrash $OUTPUT 2>/dev/null || true

echo "[4] Running Hadoop Streaming FM Algorithm..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py \
-input $INPUT/stream.txt \
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

# 13. Expected Output Format

```text
===== FM ALGORITHM RESULT =====
Total Stream Records Processed: 12
Maximum Trailing Zero Count R: 3
Estimated Number of Distinct Elements: 8
```

Actual value may vary depending on hash output.

---

# 14. Web UI

HDFS UI:

```text
http://192.168.5.35:9870
```

YARN UI:

```text
http://192.168.5.35:8088
```

Output path:

```text
/user/hadoop/cse-be/808/fm_algo/output
```

---

# Final Project Structure

```text
/home/hadoop/cse-be/808/fm_algo
├── stream.txt
├── mapper.py
├── reducer.py
└── run.sh
```

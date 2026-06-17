**DGIM Algorithm**, so below is the complete execution process and code for:

```bash
/home/hadoop/cse-be/808/dgim_algo
```

DGIM estimates the number of `1`s in a bit stream using buckets and gives approximate count with error up to 50%. 

---

# 1. Login to Master

```bash
ssh hadoop@192.168.5.35
```

---

# 2. Create Project Folder

```bash
mkdir -p /home/hadoop/cse-be/808/dgim_algo
cd /home/hadoop/cse-be/808/dgim_algo
```

---

# 3. Create Input Stream File

```bash
cat > stream.txt <<'EOF'
10101100010111011001011010101011
EOF
```

---

# 4. Create Config File

```bash
cat > config.txt <<'EOF'
32
EOF
```

---

# 5. Create Mapper Code

```bash
cat > mapper.py <<'EOF'
#!/usr/bin/env python3
import sys

timestamp = 1

for line in sys.stdin:
    line = line.strip()

    for bit in line:
        if bit in ["0", "1"]:
            print(f"{timestamp}\t{bit}")
            timestamp += 1
EOF

chmod +x mapper.py
```

---

# 6. Create Reducer Code

```bash
cat > reducer.py <<'EOF'
#!/usr/bin/env python3
import sys
import os

def read_window_size():
    if os.path.exists("config.txt"):
        with open("config.txt", "r") as f:
            return int(f.read().strip())
    return 32

N = read_window_size()
buckets = []
current_time = 0

def drop_old_buckets(current_time):
    global buckets
    cutoff = current_time - N
    buckets = [b for b in buckets if b["end"] > cutoff]

def merge_buckets():
    global buckets

    changed = True
    while changed:
        changed = False
        sizes = sorted(set(b["size"] for b in buckets))

        for size in sizes:
            same = [b for b in buckets if b["size"] == size]

            if len(same) > 2:
                same = sorted(same, key=lambda x: x["end"], reverse=True)

                oldest = same[-1]
                second_oldest = same[-2]

                buckets.remove(oldest)
                buckets.remove(second_oldest)

                merged = {
                    "size": size * 2,
                    "end": second_oldest["end"]
                }

                buckets.append(merged)
                buckets.sort(key=lambda x: x["end"], reverse=True)

                changed = True
                break

def process_bit(timestamp, bit):
    global current_time
    current_time = timestamp

    drop_old_buckets(current_time)

    if bit == "1":
        buckets.append({"size": 1, "end": timestamp})
        buckets.sort(key=lambda x: x["end"], reverse=True)
        merge_buckets()

def estimate_count():
    if not buckets:
        return 0

    total = 0

    for b in buckets[:-1]:
        total += b["size"]

    oldest = buckets[-1]
    total += oldest["size"] / 2

    return total

records = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    timestamp, bit = line.split("\t")
    records.append((int(timestamp), bit))

records.sort(key=lambda x: x[0])

for timestamp, bit in records:
    process_bit(timestamp, bit)

print("===== DGIM ALGORITHM RESULT =====")
print(f"Window Size N: {N}")
print(f"Total Stream Bits Processed: {current_time}")
print()
print("Buckets Newest to Oldest:")

for b in buckets:
    print(f"Bucket Size: {b['size']}, End Timestamp: {b['end']}")

print()
print(f"Approximate Number of 1s in last {N} bits: {estimate_count()}")
print("Note: DGIM gives approximate count with maximum error up to 50%.")
EOF

chmod +x reducer.py
```

---

# 7. Local Test

```bash
cat stream.txt | python3 mapper.py | sort -n | python3 reducer.py
```

---

# 8. Create HDFS Input Directory

```bash
hdfs dfs -mkdir -p /user/hadoop/cse-be/808/dgim_algo/input
```

---

# 9. Upload Input to HDFS

```bash
hdfs dfs -put -f stream.txt /user/hadoop/cse-be/808/dgim_algo/input/
```

---

# 10. Remove Old Output

```bash
hdfs dfs -rm -r -skipTrash /user/hadoop/cse-be/808/dgim_algo/output
```

Ignore error if output does not exist.

---

# 11. Run Hadoop Streaming Job

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py,config.txt \
-input /user/hadoop/cse-be/808/dgim_algo/input/stream.txt \
-output /user/hadoop/cse-be/808/dgim_algo/output \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py"
```

---

# 12. Check Output

```bash
hdfs dfs -cat /user/hadoop/cse-be/808/dgim_algo/output/part-00000
```

---

# 13. Create Driver File

```bash
cat > run.sh <<'EOF'
#!/bin/bash
set -e

BASE=/user/hadoop/cse-be/808/dgim_algo
INPUT=$BASE/input
OUTPUT=$BASE/output

echo "===== DGIM HADOOP PROJECT ====="

echo "[1] Creating HDFS input directory..."
hdfs dfs -mkdir -p $INPUT

echo "[2] Uploading stream.txt..."
hdfs dfs -put -f stream.txt $INPUT/

echo "[3] Removing old output..."
hdfs dfs -rm -r -skipTrash $OUTPUT 2>/dev/null || true

echo "[4] Running Hadoop Streaming DGIM job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py,config.txt \
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
/user/hadoop/cse-be/808/dgim_algo/output
```

---

# Final Project Structure

```text
/home/hadoop/cse-be/808/dgim_algo
├── stream.txt
├── config.txt
├── mapper.py
├── reducer.py
└── run.sh
```

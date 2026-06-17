Below is a complete **Arithmetic Distributed Job using Hadoop Streaming**. You will login from **PuTTY to master-a** and run the project.

Your uploaded mapper already supports operations: `ADD, SUB, MUL, DIV, MOD, POW, AVG`.  The reducer prints final operation results from mapper output. 

---

# Project Path

```bash
/home/hadoop/cse-be/808/arithmetic_dist
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
mkdir -p /home/hadoop/cse-be/808/arithmetic_dist
cd /home/hadoop/cse-be/808/arithmetic_dist
```

---

# 3. Create `mapper.py`

```bash
cat > mapper.py <<'EOF'
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split()

    operation = parts[0]
    a = float(parts[1])
    b = float(parts[2])

    if operation == "ADD":
        result = a + b
    elif operation == "SUB":
        result = a - b
    elif operation == "MUL":
        result = a * b
    elif operation == "DIV":
        result = a / b
    elif operation == "MOD":
        result = a % b
    elif operation == "POW":
        result = a ** b
    elif operation == "AVG":
        result = (a + b) / 2
    else:
        result = "INVALID_OPERATION"

    print(f"{operation}\t{result}")
EOF

chmod +x mapper.py
```

---

# 4. Create `reducer.py`

```bash
cat > reducer.py <<'EOF'
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    operation, result = line.split("\t")
    print(f"{operation}\t{result}")
EOF

chmod +x reducer.py
```

---

# 5. Create Input File

```bash
cat > input.txt <<'EOF'
ADD 10 5
SUB 20 8
MUL 6 7
DIV 40 5
MOD 17 5
POW 2 5
AVG 30 50
ADD 100 250
MUL 12 12
DIV 99 3
EOF
```

---

# 6. Test Locally First

```bash
cat input.txt | python3 mapper.py | sort | python3 reducer.py
```

Expected:

```text
ADD     15.0
ADD     350.0
AVG     40.0
DIV     8.0
DIV     33.0
MOD     2.0
MUL     42.0
MUL     144.0
POW     32.0
SUB     12.0
```

---

# 7. Create HDFS Input Directory

```bash
hdfs dfs -mkdir -p /user/hadoop/cse-be/808/arithmetic_dist/input
```

---

# 8. Upload Input File to HDFS

```bash
hdfs dfs -put -f input.txt /user/hadoop/cse-be/808/arithmetic_dist/input/
```

Verify:

```bash
hdfs dfs -ls /user/hadoop/cse-be/808/arithmetic_dist/input
```

---

# 9. Remove Old Output Directory

```bash
hdfs dfs -rm -r -skipTrash /user/hadoop/cse-be/808/arithmetic_dist/output
```

Ignore error if output does not exist.

---

# 10. Run Hadoop Streaming Job

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py \
-input /user/hadoop/cse-be/808/arithmetic_dist/input/input.txt \
-output /user/hadoop/cse-be/808/arithmetic_dist/output \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py"
```

---

# 11. Check Final Output

```bash
hdfs dfs -cat /user/hadoop/cse-be/808/arithmetic_dist/output/part-00000
```

---

# 12. Create Driver Script

```bash
cat > run.sh <<'EOF'
#!/bin/bash
set -e

BASE=/user/hadoop/cse-be/808/arithmetic_dist
INPUT=$BASE/input
OUTPUT=$BASE/output

echo "Creating HDFS input directory..."
hdfs dfs -mkdir -p $INPUT

echo "Uploading input.txt..."
hdfs dfs -put -f input.txt $INPUT/

echo "Removing old output..."
hdfs dfs -rm -r -skipTrash $OUTPUT 2>/dev/null || true

echo "Running Arithmetic Distributed Hadoop Job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py \
-input $INPUT/input.txt \
-output $OUTPUT \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py"

echo "Final Output:"
hdfs dfs -cat $OUTPUT/part-00000
EOF

chmod +x run.sh
```

Run:

```bash
./run.sh
```

---

# Web UI Check

HDFS UI:

```text
http://192.168.5.35:9870
```

YARN UI:

```text
http://192.168.5.35:8088
```

Output HDFS path:

```text
/user/hadoop/cse-be/808/arithmetic_dist/output
```

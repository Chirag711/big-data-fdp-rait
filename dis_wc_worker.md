**complete step-by-step operation** for:

**5 workers upload files directly to HDFS → master runs Hadoop Streaming Word Count → master displays final output**

Project path:

```bash
/home/hadoop/cse-be/808/dis_wc_worker
```

---

## 1. Login to master

```bash
ssh hadoop@192.168.5.35
```

or:

```bash
ssh hadoop@master-a
```

---

## 2. Create project folder on master

```bash
mkdir -p /home/hadoop/cse-be/808/dis_wc_worker
cd /home/hadoop/cse-be/808/dis_wc_worker
```

---

## 3. Create mapper.py on master

```bash
cat > mapper.py <<'EOF'
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    for word in line.strip().split():
        print(f"{word}\t1")
EOF

chmod +x mapper.py
```

---

## 4. Create reducer.py on master

```bash
cat > reducer.py <<'EOF'
#!/usr/bin/env python3
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    word, count = line.split("\t", 1)
    count = int(count)

    if current_word == word:
        current_count += count
    else:
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

if current_word is not None:
    print(f"{current_word}\t{current_count}")
EOF

chmod +x reducer.py
```

---

## 5. Create HDFS input directory from master

```bash
hdfs dfs -mkdir -p /user/hadoop/cse-be/808/dis_wc_worker/input
```

Clean old data if required:

```bash
hdfs dfs -rm -r -skipTrash /user/hadoop/cse-be/808/dis_wc_worker/input/*
```

Ignore error if directory is empty.

---

## 6. Create input file on worker-a-01

```bash
ssh hadoop@worker-a-01
```

```bash
mkdir -p /home/hadoop/cse-be/808/dis_wc_worker/input
echo "hadoop hadoop spark" > /home/hadoop/cse-be/808/dis_wc_worker/input/input1.txt
hdfs dfs -put -f /home/hadoop/cse-be/808/dis_wc_worker/input/input1.txt /user/hadoop/cse-be/808/dis_wc_worker/input/
exit
```

---

## 7. Create input file on worker-a-02

```bash
ssh hadoop@worker-a-02
```

```bash
mkdir -p /home/hadoop/cse-be/808/dis_wc_worker/input
echo "hadoop yarn hdfs" > /home/hadoop/cse-be/808/dis_wc_worker/input/input2.txt
hdfs dfs -put -f /home/hadoop/cse-be/808/dis_wc_worker/input/input2.txt /user/hadoop/cse-be/808/dis_wc_worker/input/
exit
```

---

## 8. Create input file on worker-a-03

```bash
ssh hadoop@worker-a-03
```

```bash
mkdir -p /home/hadoop/cse-be/808/dis_wc_worker/input
echo "python hadoop streaming" > /home/hadoop/cse-be/808/dis_wc_worker/input/input3.txt
hdfs dfs -put -f /home/hadoop/cse-be/808/dis_wc_worker/input/input3.txt /user/hadoop/cse-be/808/dis_wc_worker/input/
exit
```

---

## 9. Create input file on worker-a-04

```bash
ssh hadoop@worker-a-04
```

```bash
mkdir -p /home/hadoop/cse-be/808/dis_wc_worker/input
echo "spark hadoop bigdata" > /home/hadoop/cse-be/808/dis_wc_worker/input/input4.txt
hdfs dfs -put -f /home/hadoop/cse-be/808/dis_wc_worker/input/input4.txt /user/hadoop/cse-be/808/dis_wc_worker/input/
exit
```

---

## 10. Create input file on worker-a-05

```bash
ssh hadoop@worker-a-05
```

```bash
mkdir -p /home/hadoop/cse-be/808/dis_wc_worker/input
echo "hdfs yarn hadoop" > /home/hadoop/cse-be/808/dis_wc_worker/input/input5.txt
hdfs dfs -put -f /home/hadoop/cse-be/808/dis_wc_worker/input/input5.txt /user/hadoop/cse-be/808/dis_wc_worker/input/
exit
```

---

## 11. Verify all files in HDFS from master

```bash
hdfs dfs -ls /user/hadoop/cse-be/808/dis_wc_worker/input
```

Expected:

```text
input1.txt
input2.txt
input3.txt
input4.txt
input5.txt
```

Check file content:

```bash
hdfs dfs -cat /user/hadoop/cse-be/808/dis_wc_worker/input/*
```

---

## 12. Remove old output directory

```bash
hdfs dfs -rm -r -skipTrash /user/hadoop/cse-be/808/dis_wc_worker/output
```

Ignore error if output does not exist.

---

## 13. Run Hadoop Streaming job from master

```bash
cd /home/hadoop/cse-be/808/dis_wc_worker

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py \
-input /user/hadoop/cse-be/808/dis_wc_worker/input \
-output /user/hadoop/cse-be/808/dis_wc_worker/output \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py"
```

---

## 14. Check final output on master

```bash
hdfs dfs -cat /user/hadoop/cse-be/808/dis_wc_worker/output/part-00000
```

Expected output:

```text
bigdata     1
hadoop      7
hdfs        2
python      1
spark       2
streaming   1
yarn        2
```

---

## 15. Check output in HDFS UI

Open browser:

```text
http://192.168.5.35:9870
```

Go to:

```text
Utilities → Browse the file system
```

Open:

```text
/user/hadoop/cse-be/808/dis_wc_worker/output
```

Click:

```text
part-00000
```

---

## 16. Check job in YARN UI

Open:

```text
http://192.168.5.35:8088
```

Check:

```text
Applications → Your MapReduce job → SUCCEEDED
```

---

## 17. Optional driver script on master

Create:

```bash
cd /home/hadoop/cse-be/808/dis_wc_worker

cat > run_direct_hdfs.sh <<'EOF'
#!/bin/bash

set -e

HDFS_BASE=/user/hadoop/cse-be/808/dis_wc_worker
INPUT=$HDFS_BASE/input
OUTPUT=$HDFS_BASE/output

echo "Checking input files..."
hdfs dfs -ls $INPUT

echo "Removing old output..."
hdfs dfs -rm -r -skipTrash $OUTPUT 2>/dev/null || true

echo "Running Hadoop Streaming Word Count..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-files mapper.py,reducer.py \
-input $INPUT \
-output $OUTPUT \
-mapper "python3 mapper.py" \
-reducer "python3 reducer.py"

echo "Final Output:"
hdfs dfs -cat $OUTPUT/part-00000
EOF

chmod +x run_direct_hdfs.sh
```

Run anytime:

```bash
./run_direct_hdfs.sh
```

---

## Final flow

```text
Login to master
→ Create project folder
→ Create mapper.py and reducer.py
→ Create HDFS input directory
→ Login to worker-a-01 to worker-a-05
→ Create one input file on each worker
→ Each worker uploads directly to HDFS
→ Master runs Hadoop Streaming job
→ Hadoop processes files in parallel
→ Reducer writes output to HDFS
→ Master displays final result
```

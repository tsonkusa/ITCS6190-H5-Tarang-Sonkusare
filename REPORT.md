# Hands-on L5: Report

**Name:** Tarang Sonkusare
**Student ID:** 801352372
**Email:** tsonkusa@charlotte.edu

---

## What I ran

I ran the following commands in order. The PySpark statements shown in the middle were entered at the `>>>` prompt.

```bash
docker --version
docker compose up -d
docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077
```

```python
from pyspark.sql.functions import explode, split, length, col
lines = spark.read.text("/opt/spark/work-dir/shared/input/data/input.txt")
words = lines.select(explode(split(col("value"), r"\s+")).alias("word"))
counts = words.filter(length("word") >= 3).groupBy("word").count()
counts.orderBy(col("count").desc(), col("word")).show()
exit()
```

```bash
docker cp wordcount.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/wordcount.py /opt/spark/work-dir/shared/input/data/input.txt /opt/spark/work-dir/shared/output/wordcount
python3 -m py_compile wordcount.py
docker cp wordcount.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/wordcount.py /opt/spark/work-dir/shared/input/data/input.txt /opt/spark/work-dir/shared/output/wordcount-v2
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/wordcount.py /opt/spark/work-dir/shared/input/data/input.txt /opt/spark/work-dir/shared/output/wordcount-long 5
cat shared-folder/output/wordcount/part-*.txt
cat shared-folder/output/wordcount-v2/part-*.txt
cat shared-folder/output/wordcount-long/part-*.txt
wc -l shared-folder/output/wordcount/part-*.txt shared-folder/output/wordcount-v2/part-*.txt shared-folder/output/wordcount-long/part-*.txt
docker compose down
```

I followed the README in order. During my first `spark-submit` attempt, I accidentally executed only the first line of the multiline command. I corrected it by running the complete command on one line.

---

## Input and output

### My input dataset

```text
Cloud computing allows organizations to process large amounts of data using flexible computing resources. Cloud platforms make computing resources available when applications need additional capacity. Data engineers use cloud systems to store data process data and analyze data for useful business decisions.

Hadoop provides distributed storage and distributed processing for large datasets. HDFS stores data across multiple DataNodes while Hadoop coordinates computing tasks across the cluster. When data enters HDFS the file is divided into blocks and those blocks are distributed across available DataNodes.

MapReduce processes data through map shuffle and reduce phases. The mapper reads input records and produces intermediate key value pairs. The shuffle groups every value associated with the same key before the reducer starts. The reducer receives each grouped key calculates the total count and writes the final output to HDFS.
```

### The output of part 1

```text
data 7
and 6
the 6
computing 4
The 3
across 3
distributed 3
key 3
Cloud 2
HDFS 2
Hadoop 2
available 2
blocks 2
for 2
large 2
process 2
reducer 2
shuffle 2
value 2
Data 1
DataNodes 1
DataNodes. 1
HDFS. 1
MapReduce 1
When 1
additional 1
allows 1
amounts 1
analyze 1
applications 1
are 1
associated 1
before 1
business 1
calculates 1
capacity. 1
cloud 1
cluster. 1
coordinates 1
count 1
datasets. 1
decisions. 1
divided 1
each 1
engineers 1
enters 1
every 1
file 1
final 1
flexible 1
grouped 1
groups 1
input 1
intermediate 1
into 1
make 1
map 1
mapper 1
multiple 1
need 1
organizations 1
output 1
pairs. 1
phases. 1
platforms 1
processes 1
processing 1
produces 1
provides 1
reads 1
receives 1
records 1
reduce 1
resources 1
resources. 1
same 1
starts. 1
storage 1
store 1
stores 1
systems 1
tasks 1
those 1
through 1
total 1
use 1
useful 1
using 1
when 1
while 1
with 1
writes 1
```

---

## What I observed

The Spark master page showed one master and two workers in the ALIVE state. Each worker had 2 cores and 2 GB of memory. When I connected the PySpark shell, the shell appeared as a running application and two executors registered with the cluster. The `show()` operation did not run until I called it because the preceding DataFrame statements only built a plan. The stages used one task each for my single input file, while two executors were registered and available.

The interactive table showed that the original program was case-sensitive because entries such as `The` and `the`, `Data` and `data`, and `Cloud` and `cloud` appeared separately. The submitted case-insensitive run completed in approximately 7.369 seconds, and the minimum-length-5 run completed in approximately 6.343 seconds. Both submitted runs used the two registered executors.

---

## What I changed

I made the counting case-insensitive by importing `lower` and creating a lowercase word column. I replaced the hard-coded minimum of 3 with an optional command-line parameter that defaults to 3. I also added the three required counts before the table.

```python
from pyspark.sql.functions import explode, split, length, col, lower

min_length = int(sys.argv[3]) if len(sys.argv) > 3 else 3

normalized_words = words.select(lower(col("word")).alias("word"))
kept = normalized_words.filter(length("word") >= min_length)
counts = (kept.groupBy("word").count()
              .orderBy(col("count").desc(), col("word")))

print(f"{words.count()} words scanned")
print(f"{kept.count()} words of at least {min_length} characters")
print(f"{counts.count()} distinct words")
```

I also changed the usage line to document the optional argument:

```text
spark-submit wordcount.py <input file> [<output directory>] [<min word length>]
```

The existing `coalesce(1)`, output formatting, and sort order were left unchanged, so each run still produced one text part file ordered by count descending and then alphabetically by word.

---

## What the changes did

### The three numbers

| Run | Min length | Words scanned | Words kept | Distinct words |
| --- | ---: | ---: | ---: | ---: |
| `wordcount-v2` | 3 | 137 | 130 | 88 |
| `wordcount-long` | 5 | 137 | 88 | 71 |

### The three outputs compared

The original case-sensitive output contained 92 distinct entries. The case-insensitive output contained 88, so folding the case removed **4 distinct entries**. Increasing the minimum length from 3 to 5 reduced the result from 88 to 71 distinct words, so it removed **17 additional distinct words**.

For example, the original output contained `the 6` and `The 3` separately. After case folding, they became the single entry `the 9`. Similarly, `data 7` and `Data 1` became `data 8`.

### Jobs

Each modified run launched **13 Spark jobs**, numbered 0 through 12 in the log. This is more than the original program because the modified version adds actions for the total words scanned and the words that survived the length filter. Spark can also create a separate job for a shuffle stage, so the number of jobs can be greater than the number of actions written in the Python source.

Spark read and split the same file repeatedly because DataFrames are lazily evaluated plans rather than stored results. Calls such as `words.count()`, `kept.count()`, `counts.count()`, `show()`, and `write.text()` are actions. Each action triggers execution of the plan it needs. Because I did not call `cache()` or `persist()`, Spark recomputed the transformations and reread the input for later actions.

---

## Problems and fixes

My first `spark-submit` attempt returned the following usage message because I entered only the first line of the multiline command:

```text
Usage: spark-submit [options] <app jar | python file | R file> [app arguments]
```

I fixed the problem by entering the entire `spark-submit` command on one line, including the master URL, application path, input path, and output path. The application then ran successfully and created the expected output directory. I also used `python3 -m py_compile wordcount.py` after editing the program; it returned no errors.

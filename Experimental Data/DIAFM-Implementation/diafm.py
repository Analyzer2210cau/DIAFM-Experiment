from pyspark.sql import SparkSession
from pyspark import StorageLevel
import time
import psutil

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DIAFM Algorithm") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Configuration
min_support = 0.01  # Minimum support threshold
shard_size = 10000  # Number of transactions per shard

# Monitor memory and execution time
start_time = time.time()
memory_start = psutil.virtual_memory().used / (1024 * 1024)  # in MB

# Load Incremental Data (∆Dt)
data_path = "gs://your-bucket/incremental_data.csv"  # Cloud Storage path
data = spark.read.csv(data_path, header=True)

# Sharding Phase
def create_shards(row, shard_size):
    shard_id = int(row.TransactionID) // shard_size
    return shard_id, row.Items

sharded_data = data.rdd.map(lambda row: create_shards(row, shard_size))

# Persist shards for reuse
sharded_data.persist(StorageLevel.MEMORY_AND_DISK)

# Local Frequency Calculation (Map)
def local_frequency_count(shard):
    item_counts = {}
    for transaction in shard:
        items = transaction.split(", ")
        for item in items:
            item_counts[item] = item_counts.get(item, 0) + 1
    return item_counts

local_counts = sharded_data.groupByKey().mapValues(local_frequency_count)

# Profile Update (Reduce)
def merge_counts(counts1, counts2):
    for item, count in counts2.items():
        counts1[item] = counts1.get(item, 0) + count
    return counts1

global_counts = local_counts.values().reduce(merge_counts)

# Filter Frequent Items
frequent_items = {item: count for item, count in global_counts.items() if count / data.count() >= min_support}

# Save Results
output_path = "gs://your-bucket/output/frequent_items.csv"
spark.createDataFrame(frequent_items.items(), ["Item", "Count"]).write.csv(output_path, header=True)

# End Monitoring
end_time = time.time()
memory_end = psutil.virtual_memory().used / (1024 * 1024)  # in MB

print(f"Execution Time: {end_time - start_time:.2f} seconds")
print(f"Memory Used: {memory_end - memory_start:.2f} MB")

# Stop Spark Session
spark.stop()

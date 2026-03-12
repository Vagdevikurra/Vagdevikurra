## Error Diagnosis

You're hitting an **`RpcTimeoutException`** during **broadcast cleanup** (Stage 55/59). Two compounding issues:

**Root Cause 1:** `spark.rpc.askTimeout` is set *after* `getOrCreate()` — for some Spark configs, post-session setting is ignored. The error says "120 seconds" (the default), meaning your "300s" config never took effect.

**Root Cause 2:** `autoBroadcastJoinThreshold = 200MB` is too aggressive — large broadcasts are timing out when executors drop connections mid-cleanup.

---

## Fix — Update the Spark Session Builder block

Replace your current session + conf block with this:

```python
from pyspark import SparkConf

conf = (
    SparkConf()
        .setAppName("Wealth_Insights")
        # Broadcasts — reduce or disable to stop timeout during cleanup
        .set("spark.sql.adaptive.enabled",               "false")
        .set("spark.sql.autoBroadcastJoinThreshold",     "50MB")   # ← lowered from 200MB
        .set("spark.sql.broadcastTimeout",               "1200")
        .set("spark.sql.shuffle.partitions",             "600")
        # RPC / network — must be in conf BEFORE session creation
        .set("spark.rpc.askTimeout",                     "600s")   # ← was 300s, set BEFORE session
        .set("spark.network.timeout",                    "1200s")
        .set("spark.executor.heartbeatInterval",         "10s")
        .set("spark.storage.blockManagerSlaveTimeoutMs", "900000")
        # Retries
        .set("spark.task.maxFailures",                   "16")
        .set("spark.stage.maxConsecutiveAttempts",       "10")
        .set("spark.yarn.max.executor.failures",         "64")
        # File committer
        .set("mapreduce.fileoutputcommitter.algorithm.version",              "2")
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        # Speculation / blacklist
        .set("spark.speculation",                                    "false")
        .set("spark.blacklist.enabled",                              "true")
        .set("spark.blacklist.task.maxTaskAttemptsPerExecutor",      "2")
        .set("spark.blacklist.task.maxTaskAttemptsPerNode",          "2")
        .set("spark.blacklist.stage.maxFailedTasksPerExecutor",      "2")
        .set("spark.blacklist.stage.maxFailedExecutorsPerNode",      "2")
)

spark = (
    SparkSession.builder
        .config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
```

---

## Additional Fix — Unpersist broadcasts after `month_ends` cache

Add this right after the `month_ends` cache block to release broadcast memory early:

```python
spark.sql("CACHE TABLE month_ends")
spark.sql("SELECT COUNT(*) FROM month_ends").collect()

# Clear any lingering broadcasts from prior stages to free executor memory
spark.sparkContext._jvm.System.gc()
```

---

## Why the Connection Closes (TransportResponseHandler error)

The `Still have 1 requests outstanding when connection from /100.66.96.0 is closed` means an executor is being GC-evicted or killed while a broadcast unregister RPC is in-flight. Lowering the broadcast threshold to `50MB` means fewer/smaller broadcasts, so cleanup is faster and less likely to race against executor loss.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -------- Config
bronze_path = "s3://de-batch-sep2025/team_2/batch/bronze/customers/public/customers_info_team2/"
silver_path = "s3://de-batch-sep2025/team_2/batch/silver/customers_SCD/"

columns = [
    "customer_id","email","name", "loyalty_tier","address",
    "city","state","phone","updated_at"
]
columns_cdc = ["Op"] + columns
PK = "customer_id"

# -------- Read all dms files
try:
    full_df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .csv(bronze_path + "*LOAD*.csv")
        .toDF(*columns)
    )
    print(f"Full load read succeeded with {full_df.count()} rows.")
except Exception as e:
    print(f"Failed to read full load files from {bronze_path}: {str(e)}")
    job.commit()
    sys.exit(0)
    
if not full_df.rdd.isEmpty():
    # Updating Schema
    full_df = full_df.withColumn(
        "updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss.SSSSSS")
        )
    print("Showing first 5 rows of full load data:")
    full_df.show(5, truncate = False)
    # -------- Adding CDC rows to full_df
    init_dim = (
        full_df
        .filter(F.col(PK).isNotNull())
        .dropDuplicates([PK])
        .withColumn("effective_date", F.col("updated_at"))
        .withColumn("end_date", F.lit(None).cast("timestamp"))
        .withColumn("current_flag", F.lit(1))
        .withColumn("is_deleted", F.lit(0))
    )
    init_dim.write.mode("overwrite").parquet(silver_path)
    print(f"Full load completed with {init_dim.count()} rows.")
    dim_df = init_dim
    
else:
    print("No full load (LOAD) files found. Building from existing dimension.")
    try:
        dim_df = (
            spark.read
            .parquet(silver_path)
        )
    except Exception as e:
        print(f"No full load data and no existing dimension table: {str(e)}")
        print("Exiting job...")
        job.commit()
        sys.exit(0)


# -------- Moving on to CDC processing
try:
    cdc_df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .csv(bronze_path + "[0-9]*.csv")
        .toDF(*columns_cdc)
    )
    print(f"CDC read succeeded with {cdc_df.count()} rows.")
except Exception as e:
    print(f"Failed to read CDC files from {bronze_path}: {str(e)}")
    job.commit()
    sys.exit(0)
    

if not cdc_df.rdd.isEmpty():
    # Updating Schema
    cdc_df = cdc_df.withColumn(
        "updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss.SSSSSS")
        )
    print("Showing first 5 rows of CDC data:")
    cdc_df.show(5, truncate = False)
    
else:
    print("No CDC files found.")
    job.commit()
    sys.exit(0)

# -------- Cleaning the CDC files
cdc_clean = (
    cdc_df
    .filter(F.col(PK).isNotNull())
    .filter(F.col("Op").isin("I", "U", "D"))
    )
    
if cdc_clean.rdd.isEmpty():
    print("No valid CDC input after filtering")
    print("Exiting job...")
    job.commit()
    sys.exit(0)
    
# -------- Remove duplicate operations
cdc_with_ts = cdc_clean.withColumn( # Delete has no timestamp. Add it back, assuming it always wins
    "processing_ts",
    F.when(
        F.col("updated_at").isNull() & (F.col("Op") == "D"), 
        F.current_timestamp()
    ).otherwise(F.col("updated_at"))
)

op_order = (
    F.when(F.col("Op") == "D", 1)
     .when(F.col("Op") == "U", 2)
     .when(F.col("Op") == "I", 3)
)

# Ordering by descending timestamp first and then operational order
window_spec = Window.partitionBy(PK).orderBy(
    F.col("processing_ts").desc_nulls_last(),
    op_order
)

cdc_latest = (
    cdc_with_ts
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn", "processing_ts")
)

# cdc_dedup = cdc_clean.dropDuplicates(columns_cdc)

print(f"CDC records (one per customer) by {PK}: {cdc_latest.count()}")
# print(f"CDC records after dropping duplicates: {cdc_dedup.count()}")

# -------- Split CDC by I/U/D
inserts = cdc_latest.filter(F.col("Op") == "I")
updates = cdc_latest.filter(F.col("Op") == "U")
deletes = cdc_latest.filter(F.col("Op") == "D")

print("CDC counts by operation:")
print(f"\tInserts: {inserts.count()}")
print(f"\tUpdates: {updates.count()}")
print(f"\tDeletes: {deletes.count()}")


# -------- Defining an empty DF for ease of use
empty_dim = spark.createDataFrame([], dim_df.schema)

# -------- Processing INSERTS (I)
if not inserts.rdd.isEmpty():
    new_records = (
        inserts
        .drop("Op")
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("effective_date", F.current_timestamp())
        .withColumn("end_date", F.lit(None).cast("timestamp"))
        .withColumn("current_flag", F.lit(1))
        .withColumn("is_deleted", F.lit(0))
    )
    print(f"Processed {new_records.count()} INSERT records.")
else:
    new_records = empty_dim
    

# -------- Processing UPDATES (U)
if not updates.rdd.isEmpty():
    closing_old = (
        dim_df.alias("dim")
        .join(updates.select(PK).alias("cdc"), on = PK, how="inner")
        .where(F.col("dim.current_flag") == 1)
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("end_date", F.current_timestamp())
        .withColumn("current_flag", F.lit(0))
    )
    updated_new = (
        updates
        .drop("Op")
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("effective_date", F.current_timestamp())
        .withColumn("end_date", F.lit(None).cast("timestamp"))
        .withColumn("current_flag", F.lit(1))
        .withColumn("is_deleted", F.lit(0))
    )

    print(f"Processed {updated_new.count()} UPDATEs \n({closing_old.count()} OLD versions are closed).")
else:
    closing_old = empty_dim
    updated_new = empty_dim


# -------- Processing DELETES (D)
if not deletes.rdd.isEmpty():
    closed_delete = (
        dim_df.alias("dim")
        .join(deletes.select(PK).alias("cdc"), on = PK, how = "inner")
        .where(F.col("dim.current_flag") == 1)
        .select("dim.*")
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("end_date", F.current_timestamp())
        .withColumn("current_flag", F.lit(0))
        .withColumn("is_deleted", F.lit(1))
    )

    print(f"Processed {closed_delete.count()} DELETEs.")
else:
    closed_delete = empty_dim




# -------- Finalizing results
# Keys updated in the processs
cdc_keys = cdc_latest.select(PK).distinct()

# Old rows that are unaffected by this CDC batch
unchanged = (
    dim_df.alias("dim")
    .join(cdc_keys.alias("cdc"), on=PK, how="left_anti")
)

print("Final dimension composition:")
print(f"\tUnchanged:       {unchanged.count()}")
print(f"\tInserts (new):   {new_records.count()}")
print(f"\tUpdates (new):   {updated_new.count()}")
print(f"\tUpdates (closed):{closing_old.count()}")
print(f"\tDeletes (closed):{closed_delete.count()}")

final_dim = (
    unchanged
    .unionByName(closing_old, allowMissingColumns=True)
    .unionByName(updated_new, allowMissingColumns=True)
    .unionByName(new_records, allowMissingColumns=True)
    .unionByName(closed_delete, allowMissingColumns=True)
)

# -------- Write result to silver bucket
final_dim.write.mode("overwrite").parquet(silver_path)

print(f"Finished SCD2 Processing. We now have {final_dim.count()} total rows in the SCD2 table.")

print("Sample of final dimension:")
final_dim.orderBy(PK, F.col("effective_date").desc_nulls_last()).show(20, truncate=False)

job.commit()
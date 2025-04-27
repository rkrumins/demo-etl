// Spark Job 2: Fixed Column Ambiguity Issues (generate-asset.scala)
// Run with: spark-shell -i generate-asset.scala

// Import necessary libraries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.UUID

// Log lineage information
def logLineage(step: String, input: String, output: String): Unit = {
  println(s"LINEAGE: STEP=$step | INPUT=$input | OUTPUT=$output | TIME=${java.time.LocalDateTime.now}")
}

println("=== Starting Asset Creation Job ===")

// Define paths
val inputDir = "/data/lineage/processed/sales"
val outputDir = "/data/lineage/assets"
val jobId = UUID.randomUUID.toString

try {
  // Step 1: Read processed data
  logLineage("READ_PROCESSED", inputDir, "Sales DataFrame")
  
  // Try reading from Hive table first, fallback to direct parquet read
  val salesDF = if (spark.catalog.tableExists("processed_sales")) {
    println("Reading from Hive table: processed_sales")
    spark.table("processed_sales")
  } else {
    println(s"Reading directly from parquet: $inputDir")
    spark.read.parquet(inputDir)
  }
  
  println("Processed sales data schema:")
  salesDF.printSchema()
  println("Processed sales data sample:")
  salesDF.show(5)
  
  // Step 2: Create customer analytics
  logLineage("CREATE_CUSTOMER_ANALYTICS", "Sales DataFrame", "Customer Analytics")
  
  // Create customer analytics - avoid column name ambiguity
  val customerAnalyticsDF = salesDF
    .groupBy("customer_id", "customer_name")
    .agg(
      count(lit(1)).as("order_count"),
      sum("total_amount").as("total_spent"),
      max("order_date").as("last_order_date")
    )
    .withColumn("customer_value", 
      when(col("order_count") > 0, col("total_spent") / col("order_count"))
      .otherwise(0.0)
    )
    .withColumn("asset_type", lit("customer"))
    .withColumn("asset_id", concat(lit("CUST_"), col("customer_id").cast("string")))
    .withColumn("asset_created", current_timestamp())
    .withColumn("asset_job_id", lit(jobId))
  
  println("Customer analytics:")
  customerAnalyticsDF.show(5)
  
  // Step 3: Create product analytics
  logLineage("CREATE_PRODUCT_ANALYTICS", "Sales DataFrame", "Product Analytics")
  
  // Create product analytics - avoid column name ambiguity
  val productAnalyticsDF = salesDF
    .groupBy("product_id", "product_name", "category")
    .agg(
      count(lit(1)).as("order_count"),
      sum("quantity").as("units_sold"),
      sum("extended_price").as("total_revenue")
    )
    .withColumn("avg_unit_price", 
      when(col("units_sold") > 0, col("total_revenue") / col("units_sold"))
      .otherwise(0.0)
    )
    .withColumn("asset_type", lit("product"))
    .withColumn("asset_id", concat(lit("PROD_"), col("product_id").cast("string")))
    .withColumn("asset_created", current_timestamp())
    .withColumn("asset_job_id", lit(jobId))
  
  println("Product analytics:")
  productAnalyticsDF.show(5)
  
  // Step 4: Create time analytics
  logLineage("CREATE_TIME_ANALYTICS", "Sales DataFrame", "Time Analytics")
  
  // Create time analytics - avoid column name ambiguity
  val timeAnalyticsDF = salesDF
    .groupBy("month", "year")
    .agg(
      count(lit(1)).as("order_count"),
      countDistinct("customer_id").as("unique_customers"),
      sum("total_amount").as("total_revenue")
    )
    .withColumn("avg_order_value", 
      when(col("order_count") > 0, col("total_revenue") / col("order_count"))
      .otherwise(0.0)
    )
    .withColumn("asset_type", lit("time"))
    .withColumn("asset_id", concat(lit("TIME_"), col("year").cast("string"), lit("_"), col("month")))
    .withColumn("asset_created", current_timestamp())
    .withColumn("asset_job_id", lit(jobId))
  
  println("Time analytics:")
  timeAnalyticsDF.show(5)
  
  // Step 5: Create unified asset - avoid column ambiguity by selecting explicitly
  logLineage("CREATE_UNIFIED_ASSET", "All Analytics DataFrames", "Unified Asset")
  
  // Define schema for the unified asset to ensure consistent types
  val unifiedSchema = StructType(Array(
    StructField("asset_type", StringType, false),
    StructField("asset_id", StringType, false),
    StructField("asset_created", TimestampType, false),
    StructField("asset_job_id", StringType, false),
    StructField("customer_id", IntegerType, true),
    StructField("customer_name", StringType, true),
    StructField("order_count", LongType, true),
    StructField("total_spent", DoubleType, true),
    StructField("product_id", IntegerType, true),
    StructField("product_name", StringType, true),
    StructField("category", StringType, true),
    StructField("month", StringType, true),
    StructField("year", IntegerType, true)
  ))
  
  // Select from customer analytics with explicit schema
  val customerColumns = customerAnalyticsDF.select(
    col("asset_type"),
    col("asset_id"),
    col("asset_created"),
    col("asset_job_id"),
    col("customer_id"),
    col("customer_name"),
    col("order_count"),
    col("total_spent"),
    lit(null).cast(IntegerType).as("product_id"),
    lit(null).cast(StringType).as("product_name"),
    lit(null).cast(StringType).as("category"),
    lit(null).cast(StringType).as("month"),
    lit(null).cast(IntegerType).as("year")
  )
  
  // Select from product analytics with explicit schema
  val productColumns = productAnalyticsDF.select(
    col("asset_type"),
    col("asset_id"),
    col("asset_created"),
    col("asset_job_id"),
    lit(null).cast(IntegerType).as("customer_id"),
    lit(null).cast(StringType).as("customer_name"),
    col("order_count"),
    lit(null).cast(DoubleType).as("total_spent"),
    col("product_id"),
    col("product_name"),
    col("category"),
    lit(null).cast(StringType).as("month"),
    lit(null).cast(IntegerType).as("year")
  )
  
  // Select from time analytics with explicit schema
  val timeColumns = timeAnalyticsDF.select(
    col("asset_type"),
    col("asset_id"),
    col("asset_created"),
    col("asset_job_id"),
    lit(null).cast(IntegerType).as("customer_id"),
    lit(null).cast(StringType).as("customer_name"),
    col("order_count"),
    lit(null).cast(DoubleType).as("total_spent"),
    lit(null).cast(IntegerType).as("product_id"),
    lit(null).cast(StringType).as("product_name"),
    lit(null).cast(StringType).as("category"),
    col("month"),
    col("year")
  )
  
  // Use standard union to combine all assets
  val unifiedAssetDF = customerColumns
    .union(productColumns)
    .union(timeColumns)
    .withColumn("asset_source", lit(inputDir))
  
  println("Unified asset:")
  unifiedAssetDF.show(10)
  
  // Step 6: Write asset data
  logLineage("WRITE_ASSET", "Unified Asset", outputDir)
  
  // Ensure output directory is clean
  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  val outputPath = new org.apache.hadoop.fs.Path(outputDir)
  if (hdfs.exists(outputPath)) {
    hdfs.delete(outputPath, true)
    println(s"Deleted existing directory: $outputDir")
  }
  
  // Write data
  unifiedAssetDF.write
    .mode("overwrite")
    .parquet(outputDir)
  
  println(s"Asset data written to: $outputDir")
  println(s"Total asset records: ${unifiedAssetDF.count()}")
  
  // Create Hive table for the asset
  spark.sql("DROP TABLE IF EXISTS sales_analytics_asset")
  spark.sql(s"""
    CREATE TABLE sales_analytics_asset
    USING PARQUET
    LOCATION '$outputDir'
  """)
  
  println("Created Hive table: sales_analytics_asset")
  
  // Create lineage registry table and add entry
  spark.sql("CREATE TABLE IF NOT EXISTS lineage_registry (job_id STRING, job_time TIMESTAMP, source_path STRING, target_path STRING)")
  
  val lineageDF = Seq(
    (jobId, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now()), inputDir, outputDir)
  ).toDF("job_id", "job_time", "source_path", "target_path")
  
  lineageDF.write.mode("append").insertInto("lineage_registry")
  
  println("Added entry to lineage registry")
  println("Lineage registry contents:")
  spark.sql("SELECT * FROM lineage_registry").show()
  
  println("=== Asset Creation Job Complete ===")
  
} catch {
  case e: Exception =>
    println(s"ERROR: ${e.getMessage}")
    e.printStackTrace()
    throw e
}

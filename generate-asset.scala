// Fixed Spark Job 2: Asset Creation (generate-asset.scala)
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
  
  println("Checking if Hive table exists...")
  val tableExists = spark.catalog.tableExists("processed_sales")
  println(s"Table exists: $tableExists")
  
  // Try reading from Hive table first, fallback to direct parquet read
  val salesDF = if (tableExists) {
    println("Reading from Hive table: processed_sales")
    val df = spark.table("processed_sales")
    println(s"Read ${df.count()} records from Hive table")
    df
  } else {
    println(s"Reading directly from parquet: $inputDir")
    // Check if path exists
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val inputPath = new org.apache.hadoop.fs.Path(inputDir)
    
    if (!hdfs.exists(inputPath)) {
      throw new Exception(s"Input path not found: $inputDir. Please run Job 1 first.")
    }
    
    val df = spark.read.parquet(inputDir)
    println(s"Read ${df.count()} records from parquet files")
    df
  }
  
  // Show schema to verify column names and types for joins
  println("Processed sales data schema:")
  salesDF.printSchema()
  
  println("Processed sales data sample:")
  salesDF.show(5)
  
  // Step 2: Create customer analytics
  logLineage("CREATE_CUSTOMER_ANALYTICS", "Sales DataFrame", "Customer Analytics")
  
  // Check that expected columns exist
  val hasRequiredColumns = salesDF.columns.toSet.intersect(
    Set("customer_id", "customer_name", "order_id", "total_amount", "order_date")
  ).size == 5
  
  if (!hasRequiredColumns) {
    throw new Exception("Sales data is missing required columns for customer analytics")
  }
  
  // Create customer analytics with explicit aggregate functions
  val customerAnalyticsDF = salesDF
    .groupBy(col("customer_id"), col("customer_name"))
    .agg(
      count(col("order_id")).as("order_count"),
      sum(col("total_amount")).as("total_spent"),
      max(col("order_date")).as("last_order_date")
    )
    // Safe division to avoid divide-by-zero errors
    .withColumn("customer_value", 
      when(col("order_count") > 0, col("total_spent") / col("order_count"))
      .otherwise(0.0)
    )
    .withColumn("asset_type", lit("customer"))
    .withColumn("asset_id", concat(lit("CUST_"), col("customer_id").cast("string")))
    .withColumn("asset_created", current_timestamp())
    .withColumn("asset_job_id", lit(jobId))
  
  println("Customer analytics:")
  println(s"Customer records: ${customerAnalyticsDF.count()}")
  customerAnalyticsDF.show(5)
  
  // Step 3: Create product analytics
  logLineage("CREATE_PRODUCT_ANALYTICS", "Sales DataFrame", "Product Analytics")
  
  // Check that expected columns exist
  val hasProductColumns = salesDF.columns.toSet.intersect(
    Set("product_id", "product_name", "category", "quantity", "extended_price")
  ).size == 5
  
  if (!hasProductColumns) {
    throw new Exception("Sales data is missing required columns for product analytics")
  }
  
  val productAnalyticsDF = salesDF
    .groupBy(col("product_id"), col("product_name"), col("category"))
    .agg(
      count(col("order_id")).as("order_count"),
      sum(col("quantity")).as("units_sold"),
      sum(col("extended_price")).as("total_revenue")
    )
    // Safe division to avoid divide-by-zero errors
    .withColumn("avg_unit_price", 
      when(col("units_sold") > 0, col("total_revenue") / col("units_sold"))
      .otherwise(0.0)
    )
    .withColumn("asset_type", lit("product"))
    .withColumn("asset_id", concat(lit("PROD_"), col("product_id").cast("string")))
    .withColumn("asset_created", current_timestamp())
    .withColumn("asset_job_id", lit(jobId))
  
  println("Product analytics:")
  println(s"Product records: ${productAnalyticsDF.count()}")
  productAnalyticsDF.show(5)
  
  // Step 4: Create time analytics
  logLineage("CREATE_TIME_ANALYTICS", "Sales DataFrame", "Time Analytics")
  
  // Check that expected columns exist
  val hasTimeColumns = salesDF.columns.toSet.intersect(
    Set("month", "year", "order_id", "customer_id", "total_amount")
  ).size == 5
  
  if (!hasTimeColumns) {
    throw new Exception("Sales data is missing required columns for time analytics")
  }
  
  val timeAnalyticsDF = salesDF
    .groupBy(col("month"), col("year"))
    .agg(
      count(col("order_id")).as("order_count"),
      countDistinct(col("customer_id")).as("unique_customers"),
      sum(col("total_amount")).as("total_revenue")
    )
    // Safe division to avoid divide-by-zero errors
    .withColumn("avg_order_value", 
      when(col("order_count") > 0, col("total_revenue") / col("order_count"))
      .otherwise(0.0)
    )
    .withColumn("asset_type", lit("time"))
    .withColumn("asset_id", concat(lit("TIME_"), col("year").cast("string"), lit("_"), col("month")))
    .withColumn("asset_created", current_timestamp())
    .withColumn("asset_job_id", lit(jobId))
  
  println("Time analytics:")
  println(s"Time period records: ${timeAnalyticsDF.count()}")
  timeAnalyticsDF.show(5)
  
  // Step 5: Create unified asset
  logLineage("CREATE_UNIFIED_ASSET", "All Analytics DataFrames", "Unified Asset")
  
  // Cast all common columns to ensure consistent types for union
  // This prevents "Failed to merge incompatible data types" errors
  val customerColumns = customerAnalyticsDF.select(
    col("asset_type").cast(StringType), 
    col("asset_id").cast(StringType), 
    col("asset_created").cast(TimestampType), 
    col("asset_job_id").cast(StringType),
    col("customer_id").cast(IntegerType), 
    col("customer_name").cast(StringType), 
    col("order_count").cast(LongType), 
    col("total_spent").cast(DoubleType),
    lit(null).cast(IntegerType).as("product_id"), 
    lit(null).cast(StringType).as("product_name"), 
    lit(null).cast(StringType).as("category"),
    lit(null).cast(StringType).as("month"), 
    lit(null).cast(IntegerType).as("year")
  )
  
  val productColumns = productAnalyticsDF.select(
    col("asset_type").cast(StringType), 
    col("asset_id").cast(StringType), 
    col("asset_created").cast(TimestampType), 
    col("asset_job_id").cast(StringType),
    lit(null).cast(IntegerType).as("customer_id"), 
    lit(null).cast(StringType).as("customer_name"), 
    col("order_count").cast(LongType), 
    lit(null).cast(DoubleType).as("total_spent"),
    col("product_id").cast(IntegerType), 
    col("product_name").cast(StringType), 
    col("category").cast(StringType),
    lit(null).cast(StringType).as("month"), 
    lit(null).cast(IntegerType).as("year")
  )
  
  val timeColumns = timeAnalyticsDF.select(
    col("asset_type").cast(StringType), 
    col("asset_id").cast(StringType), 
    col("asset_created").cast(TimestampType), 
    col("asset_job_id").cast(StringType),
    lit(null).cast(IntegerType).as("customer_id"), 
    lit(null).cast(StringType).as("customer_name"), 
    col("order_count").cast(LongType), 
    lit(null).cast(DoubleType).as("total_spent"),
    lit(null).cast(IntegerType).as("product_id"), 
    lit(null).cast(StringType).as("product_name"), 
    lit(null).cast(StringType).as("category"),
    col("month").cast(StringType), 
    col("year").cast(IntegerType)
  )
  
  // Union all assets
  // In Spark 3.x, we can use unionByName instead for better column matching
  val sparkVersion = spark.version
  
  val unifiedAssetDF = if (sparkVersion.startsWith("3.")) {
    println("Using unionByName for Spark 3.x")
    // Use unionByName with allowMissingColumns=true for Spark 3.x
    customerColumns
      .unionByName(productColumns, true)
      .unionByName(timeColumns, true)
      .withColumn("asset_source", lit(inputDir))
  } else {
    println("Using standard union for Spark 2.x")
    // For Spark 2.x, use regular union with explicit column selection
    customerColumns
      .union(productColumns)
      .union(timeColumns)
      .withColumn("asset_source", lit(inputDir))
  }
  
  println("Unified asset:")
  println(s"Total asset records: ${unifiedAssetDF.count()}")
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
  
  // Verify written data by reading it back
  val verifyDF = spark.read.parquet(outputDir)
  println("Verification of written asset data:")
  println(s"Total asset records: ${verifyDF.count()}")
  verifyDF.show(3)
  
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
    
    // Log lineage error for tracking failures
    logLineage("ERROR", "Failed process", e.getMessage)
    
    throw e // Re-throw to ensure the script fails properly
}

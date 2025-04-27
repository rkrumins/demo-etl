// Fixed Spark Shell Script 2: Asset Creation with Resolved Column Ambiguity
// Run with: spark-shell -i spark-shell-job2-fixed.scala

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import java.time.{LocalDate, LocalDateTime}
import java.sql.{Date, Timestamp}
import java.util.UUID

// Function to log lineage information
def logLineage(description: String, inputs: Seq[String], output: String): Unit = {
  println(s"LINEAGE: $description")
  println(s"LINEAGE: Inputs: ${inputs.mkString(", ")}")
  println(s"LINEAGE: Output: $output")
  println(s"LINEAGE: Timestamp: ${java.time.LocalDateTime.now}")
  println("LINEAGE: " + "=" * 50)
}

println("Starting Asset Creation Job from Spark Shell")

// External table location (from first job)
val externalTableLocation = "/user/hive/external/processed_sales"

// Asset table location
val assetTableLocation = "/user/hive/warehouse/sales_analytics_asset"

// Metadata for lineage tracking
val jobId = UUID.randomUUID.toString
val jobStartTime = LocalDateTime.now

println(s"Job ID: $jobId")
println(s"Start Time: $jobStartTime")
println(s"Reading from external table at: $externalTableLocation")
println(s"Writing to asset table at: $assetTableLocation")

try {
  // Read from external table
  println("Reading data from external Hive table...")
  
  // First check if the external table exists
  println("Checking if external table exists...")
  val tableExists = spark.catalog.tableExists("sales_data_external")
  
  val salesDataDF = if (tableExists) {
    println("Reading from Hive table 'sales_data_external'")
    spark.table("sales_data_external")
  } else {
    println("Reading directly from parquet files at: " + externalTableLocation)
    spark.read.format("parquet").load(externalTableLocation)
  }
  
  // Show table information
  println("External table schema:")
  salesDataDF.printSchema()
  
  println("Sample data from external table:")
  salesDataDF.show(5)
  
  println(s"Total records in external table: ${salesDataDF.count()}")
  
  // Cache the data for better performance
  salesDataDF.persist(StorageLevel.MEMORY_AND_DISK)
  
  // Log lineage for data loading from external table
  logLineage(
    "Load Data from External Table", 
    Seq(s"External Table: sales_data_external at $externalTableLocation"), 
    "salesDataDF"
  )
  
  // TRANSFORMATION 1: Extract customer analytics
  println("Creating customer analytics...")
  
  // Make sure to use fully qualified column references
  val customerAnalyticsDF = salesDataDF
    .filter(salesDataDF("customer_id").isNotNull) // Filter out summary rows
    .groupBy(
      salesDataDF("customer_id"),
      salesDataDF("customer_name")
    )
    .agg(
      count(salesDataDF("order_id")).as("total_orders"),
      countDistinct(salesDataDF("product_id")).as("unique_products_purchased"),
      sum(salesDataDF("final_price")).as("total_spent"),
      max(salesDataDF("order_date")).as("last_purchase_date"),
      min(salesDataDF("order_date")).as("first_purchase_date"),
      avg(salesDataDF("final_price") / salesDataDF("quantity")).as("avg_unit_price"),
      sum(when(salesDataDF("is_weekend") === true, salesDataDF("final_price")).otherwise(0)).as("weekend_spending"),
      sum(when(salesDataDF("is_weekend") === false, salesDataDF("final_price")).otherwise(0)).as("weekday_spending")
    )
    
  // Add calculated customer metrics with explicit references
  val enhancedCustomerAnalyticsDF = customerAnalyticsDF
    .withColumn("days_as_customer", datediff(current_date(), col("first_purchase_date")))
    .withColumn("days_since_last_purchase", datediff(current_date(), col("last_purchase_date")))
    .withColumn("purchase_frequency_days", 
      when(col("total_orders") > 1, 
          col("days_as_customer") / (col("total_orders") - 1)
      ).otherwise(null))
    .withColumn("customer_value_score", 
      (col("total_spent") / lit(100)) * 
      (lit(1) + when(col("days_since_last_purchase") < 30, 0.5).otherwise(-0.2)) *
      (when(col("total_orders") > 10, 1.2).otherwise(1.0))
    )
    .withColumn("weekend_shopper_ratio", 
      when(col("weekend_spending") + col("weekday_spending") > 0,
        col("weekend_spending") / (col("weekend_spending") + col("weekday_spending"))
      ).otherwise(0)
    )
  
  // Show customer analytics
  println("Sample customer analytics:")
  enhancedCustomerAnalyticsDF.select("customer_id", "customer_name", "total_orders", "total_spent", "customer_value_score").show(5)
  
  // Log lineage for customer analytics transformation
  logLineage(
    "Extract and Enhance Customer Analytics", 
    Seq("salesDataDF"), 
    "enhancedCustomerAnalyticsDF"
  )
  
  // TRANSFORMATION 2: Extract product analytics
  println("Creating product analytics...")
  val productAnalyticsDF = salesDataDF
    .filter(salesDataDF("product_id").isNotNull) // Filter out summary rows
    .groupBy(
      salesDataDF("product_id"), 
      salesDataDF("product_name"), 
      salesDataDF("category")
    )
    .agg(
      count(salesDataDF("order_id")).as("order_count"),
      sum(salesDataDF("quantity")).as("total_quantity_sold"),
      sum(salesDataDF("final_price")).as("total_revenue"),
      avg(salesDataDF("price")).as("avg_price"),
      avg(when(salesDataDF("discount_amount") > 0 && salesDataDF("extended_price") > 0,
        salesDataDF("discount_amount") / salesDataDF("extended_price")
      ).otherwise(0)).as("avg_discount_rate"),
      countDistinct(salesDataDF("customer_id")).as("unique_customers")
    )
    .withColumn("revenue_per_unit", 
      when(col("total_quantity_sold") > 0, 
        col("total_revenue") / col("total_quantity_sold")
      ).otherwise(0)
    )
    
  // Add product ranking within category
  val windowByCategoryRevenue = Window.partitionBy(col("category")).orderBy(col("total_revenue").desc)
  val rankedProductAnalyticsDF = productAnalyticsDF
    .withColumn("revenue_rank_in_category", rank().over(windowByCategoryRevenue))
    .withColumn("percent_rank_in_category", percent_rank().over(windowByCategoryRevenue))
    .withColumn("is_top_seller", col("revenue_rank_in_category") <= 3)
  
  // Show product analytics
  println("Sample product analytics with ranking:")
  rankedProductAnalyticsDF.select("product_id", "product_name", "category", "total_revenue", "revenue_rank_in_category", "is_top_seller").show(5)
  
  // Log lineage for product analytics transformation
  logLineage(
    "Extract and Rank Product Analytics", 
    Seq("salesDataDF"), 
    "rankedProductAnalyticsDF"
  )
  
  // TRANSFORMATION 3: Extract time-based analytics
  println("Creating time-based analytics...")
  val timeAnalyticsDF = salesDataDF
    .filter(salesDataDF("order_date").isNotNull) // Filter out summary rows
    .withColumn("date", to_date(salesDataDF("order_date")))
    .groupBy(
      col("date"), 
      salesDataDF("day_of_week"), 
      salesDataDF("month"), 
      salesDataDF("year")
    )
    .agg(
      count(salesDataDF("order_id")).as("order_count"),
      countDistinct(salesDataDF("customer_id")).as("unique_customers"),
      sum(salesDataDF("final_price")).as("total_revenue"),
      avg(salesDataDF("final_price")).as("avg_order_value"),
      sum(salesDataDF("quantity")).as("items_sold"),
      countDistinct(salesDataDF("product_id")).as("unique_products_sold")
    )
    .withColumn("revenue_per_customer", 
      when(col("unique_customers") > 0, 
        col("total_revenue") / col("unique_customers")
      ).otherwise(0)
    )
    .withColumn("items_per_order", 
      when(col("order_count") > 0, 
        col("items_sold") / col("order_count")
      ).otherwise(0)
    )
    
  // Make sure we have at least 7 days of data for meaningful trends
  // This is important for our sample data which might not have enough history
  val minDate = timeAnalyticsDF.agg(min(col("date"))).first().getDate(0)
  val maxDate = timeAnalyticsDF.agg(max(col("date"))).first().getDate(0)
  
  println(s"Date range in data: $minDate to $maxDate")
  
  // Create a complete date series to fill any gaps
  val dateRange = (0 until 100).map(i => 
    Date.valueOf(LocalDate.now().minusDays(i))
  ).toDF("date")
  
  // Join with our time analytics to ensure no gaps
  val completeTimeAnalyticsDF = dateRange
    .join(timeAnalyticsDF, Seq("date"), "left")
    .na.fill(0, Seq("order_count", "unique_customers", "total_revenue", "items_sold"))
    .na.fill("Unknown", Seq("day_of_week", "month"))
    .orderBy(col("date"))
  
  // Calculate moving averages for trending
  // Adjust window size based on available data
  val windowSpec3Day = Window
    .orderBy(col("date"))
    .rangeBetween(-2, 0) // 3-day window
    
  val windowSpec7Day = Window
    .orderBy(col("date"))
    .rangeBetween(-6, 0) // 7-day window
    
  val trendingTimeAnalyticsDF = completeTimeAnalyticsDF
    .withColumn("revenue_3day_avg", avg(col("total_revenue")).over(windowSpec3Day))
    .withColumn("revenue_7day_avg", avg(col("total_revenue")).over(windowSpec7Day))
    .withColumn("order_count_3day_avg", avg(col("order_count")).over(windowSpec3Day))
    .withColumn("order_count_7day_avg", avg(col("order_count")).over(windowSpec7Day))
    .withColumn("is_revenue_trending_up", 
      when(col("revenue_3day_avg") > col("revenue_7day_avg") * 1.05, true)  // 5% higher than 7-day average
      .otherwise(false)
    )
  
  // Show time analytics
  println("Sample time-based analytics with trending:")
  trendingTimeAnalyticsDF
    .filter(col("total_revenue") > 0)  // Only show days with revenue
    .select("date", "total_revenue", "revenue_3day_avg", "revenue_7day_avg", "is_revenue_trending_up")
    .orderBy(col("date").desc)
    .show(5)
  
  // Log lineage for time analytics transformation  
  logLineage(
    "Extract Time-Based Analytics with Trends", 
    Seq("salesDataDF"), 
    "trendingTimeAnalyticsDF"
  )
  
  // TRANSFORMATION 4: Create assets by joining all analytics perspectives
  println("Creating unified asset view...")
  
  // First, create a date dimension to join with
  val today = LocalDate.now()
  val dateDf = (0 until 365).map(i => {
    val date = today.minusDays(i)
    (Date.valueOf(date),
     date.getDayOfWeek.toString,
     date.getMonthValue,
     date.getMonth.toString,
     date.getYear)
  }).toDF("date", "day_of_week", "month_num", "month_name", "year")
  
  // Prepare for joining to the date dimension - use specific column references
  val timeAnalyticsForJoin = trendingTimeAnalyticsDF
    .withColumnRenamed("month", "month_name")
    .join(dateDf, Seq("date"), "left")
    .select(
      trendingTimeAnalyticsDF("date"),
      dateDf("day_of_week"),
      dateDf("month_num"),
      dateDf("month_name"),
      dateDf("year"),
      trendingTimeAnalyticsDF("order_count"),
      trendingTimeAnalyticsDF("unique_customers"),
      trendingTimeAnalyticsDF("total_revenue"),
      trendingTimeAnalyticsDF("avg_order_value"),
      trendingTimeAnalyticsDF("items_sold"),
      trendingTimeAnalyticsDF("unique_products_sold"),
      trendingTimeAnalyticsDF("revenue_per_customer"),
      trendingTimeAnalyticsDF("items_per_order"),
      trendingTimeAnalyticsDF("revenue_3day_avg"),
      trendingTimeAnalyticsDF("revenue_7day_avg"),
      trendingTimeAnalyticsDF("order_count_3day_avg"),
      trendingTimeAnalyticsDF("order_count_7day_avg"),
      trendingTimeAnalyticsDF("is_revenue_trending_up")
    )
  
  // Create customer dimension asset
  val customerAssetDF = enhancedCustomerAnalyticsDF
    .withColumn("asset_type", lit("customer"))
    .withColumn("asset_id", concat(lit("CUST_"), col("customer_id")))
    .withColumn("asset_name", col("customer_name"))
    .withColumn("asset_created_date", current_timestamp())
    .withColumn("asset_job_id", lit(jobId))
  
  // Create product dimension asset
  val productAssetDF = rankedProductAnalyticsDF
    .withColumn("asset_type", lit("product"))
    .withColumn("asset_id", concat(lit("PROD_"), col("product_id")))
    .withColumn("asset_name", col("product_name"))
    .withColumn("asset_created_date", current_timestamp())
    .withColumn("asset_job_id", lit(jobId))
  
  // Create time dimension asset
  val timeAssetDF = timeAnalyticsForJoin
    .withColumn("asset_type", lit("time"))
    .withColumn("asset_id", concat(lit("DATE_"), date_format(col("date"), "yyyyMMdd")))
    .withColumn("asset_name", date_format(col("date"), "yyyy-MM-dd"))
    .withColumn("asset_created_date", current_timestamp())
    .withColumn("asset_job_id", lit(jobId))
  
  // Log lineage for asset transformation
  logLineage(
    "Create Dimension Assets", 
    Seq("enhancedCustomerAnalyticsDF", "rankedProductAnalyticsDF", "trendingTimeAnalyticsDF"), 
    "Multiple Dimension Assets"
  )
  
  // Construct the unified asset view
  println("Creating unified asset schema...")
  
  // Customer columns
  val customerColumns = customerAssetDF.select(
    col("asset_type"), col("asset_id"), col("asset_name"), col("asset_created_date"), col("asset_job_id"),
    col("customer_id"), col("total_orders"), col("total_spent"), col("customer_value_score"), 
    col("first_purchase_date"), col("last_purchase_date"), col("days_since_last_purchase"),
    lit(null).cast("string").as("category"),
    lit(null).cast("int").as("revenue_rank_in_category"),
    lit(null).cast("date").as("date"),
    lit(null).cast("string").as("day_of_week"),
    lit(null).cast("int").as("month_num"),
    lit(null).cast("string").as("month_name"),
    lit(null).cast("int").as("year"),
    lit(null).cast("double").as("total_revenue"),
    lit(null).cast("boolean").as("is_revenue_trending_up")
  )
  
  // Product columns
  val productColumns = productAssetDF.select(
    col("asset_type"), col("asset_id"), col("asset_name"), col("asset_created_date"), col("asset_job_id"),
    lit(null).cast("int").as("customer_id"),
    lit(null).cast("int").as("total_orders"),
    lit(null).cast("double").as("total_spent"),
    lit(null).cast("double").as("customer_value_score"),
    lit(null).cast("timestamp").as("first_purchase_date"),
    lit(null).cast("timestamp").as("last_purchase_date"),
    lit(null).cast("int").as("days_since_last_purchase"),
    col("category"), col("revenue_rank_in_category"), 
    lit(null).cast("date").as("date"),
    lit(null).cast("string").as("day_of_week"),
    lit(null).cast("int").as("month_num"),
    lit(null).cast("string").as("month_name"),
    lit(null).cast("int").as("year"),
    col("total_revenue"),
    lit(null).cast("boolean").as("is_revenue_trending_up")
  )
  
  // Time columns
  val timeColumns = timeAssetDF.select(
    col("asset_type"), col("asset_id"), col("asset_name"), col("asset_created_date"), col("asset_job_id"),
    lit(null).cast("int").as("customer_id"),
    col("order_count").as("total_orders"),
    lit(null).cast("double").as("total_spent"),
    lit(null).cast("double").as("customer_value_score"),
    lit(null).cast("timestamp").as("first_purchase_date"),
    lit(null).cast("timestamp").as("last_purchase_date"),
    lit(null).cast("int").as("days_since_last_purchase"),
    lit(null).cast("string").as("category"),
    lit(null).cast("int").as("revenue_rank_in_category"),
    col("date"), col("day_of_week"), col("month_num"), col("month_name"), col("year"),
    col("total_revenue"), col("is_revenue_trending_up")
  )
  
  // Union all asset types into a unified view
  val unifiedAssetDF = customerColumns
    .union(productColumns)
    .union(timeColumns)
    .withColumn("asset_lineage_source", lit(externalTableLocation))
    .withColumn("asset_lineage_job", lit(jobId))
  
  // Show sample unified asset data
  println("Sample unified asset data:")
  unifiedAssetDF.select("asset_type", "asset_id", "asset_name", "asset_job_id").show(5)
  
  println(s"Total records in unified asset: ${unifiedAssetDF.count()}")
  
  // Log lineage for unified asset creation
  logLineage(
    "Create Unified Asset View", 
    Seq("customerAssetDF", "productAssetDF", "timeAssetDF"), 
    "unifiedAssetDF"
  )
  
  // Create the target Hive table for the asset
  println("Creating asset table in Hive...")
  spark.sql("DROP TABLE IF EXISTS sales_analytics_asset")
  spark.sql(
    s"""
      |CREATE TABLE sales_analytics_asset (
      |  asset_type STRING,
      |  asset_id STRING,
      |  asset_name STRING,
      |  asset_created_date TIMESTAMP,
      |  asset_job_id STRING,
      |  customer_id INT,
      |  total_orders INT,
      |  total_spent DOUBLE,
      |  customer_value_score DOUBLE,
      |  first_purchase_date TIMESTAMP,
      |  last_purchase_date TIMESTAMP,
      |  days_since_last_purchase INT,
      |  category STRING,
      |  revenue_rank_in_category INT,
      |  date DATE,
      |  day_of_week STRING,
      |  month_num INT,
      |  month_name STRING,
      |  year INT,
      |  total_revenue DOUBLE,
      |  is_revenue_trending_up BOOLEAN,
      |  asset_lineage_source STRING,
      |  asset_lineage_job STRING
      |)
      |STORED AS PARQUET
      |LOCATION '$assetTableLocation'
    """.stripMargin
  )
  
  // Write the asset data
  println("Writing data to asset table...")
  unifiedAssetDF.write
    .mode("overwrite")
    .format("parquet")
    .save(assetTableLocation)
  
  // Verify the data was written correctly
  val verifyAssetDF = spark.sql("SELECT * FROM sales_analytics_asset LIMIT 10")
  println("Verifying data in asset table:")
  verifyAssetDF.show(5)
  
  // Count the records in the asset table
  val assetRecordCount = spark.sql("SELECT COUNT(*) FROM sales_analytics_asset").first().getLong(0)
  println(s"Total records in asset table: $assetRecordCount")
  
  // Log lineage for writing the asset
  logLineage(
    "Write Unified Asset to Hive Table", 
    Seq("unifiedAssetDF"), 
    s"Hive Table: sales_analytics_asset at $assetTableLocation"
  )
  
  // Create lineage table in Hive to explicitly track data flow
  println("Creating data lineage table in Hive...")
  spark.sql("CREATE TABLE IF NOT EXISTS data_lineage_registry (job_id STRING, job_timestamp TIMESTAMP, source_path STRING, target_path STRING, transformation_count INT, record_count BIGINT, source_files STRING, description STRING)")
  
  // Build lineage data with explicit source reference to the first job's output
  val lineageData = Seq(
    (
      jobId, 
      Timestamp.valueOf(jobStartTime), 
      externalTableLocation, 
      assetTableLocation, 
      4, // number of transformations in this job
      unifiedAssetDF.count(),
      "sales_data_external",
      s"Created sales analytics asset from processed sales data at ${LocalDateTime.now}"
    )
  ).toDF("job_id", "job_timestamp", "source_path", "target_path", "transformation_count", "record_count", "source_files", "description")
  
  // Insert lineage data into registry
  println("Registering lineage metadata...")
  lineageData.write
    .mode("append")
    .format("hive")
    .saveAsTable("data_lineage_registry")
  
  // Final lineage log
  logLineage(
    "Register Data Lineage", 
    Seq(externalTableLocation), 
    "data_lineage_registry table entry"
  )
  
  // Unpersist cached DataFrame
  salesDataDF.unpersist()
  
  // Print completion message with lineage summary
  val jobEndTime = LocalDateTime.now
  println(s"""
    |==========================================================
    |JOB COMPLETE: Asset Creation with Lineage Tracking
    |==========================================================
    |Job ID: $jobId
    |Start Time: $jobStartTime
    |End Time: $jobEndTime
    |Duration: ${java.time.Duration.between(jobStartTime, jobEndTime).getSeconds} seconds
    |
    |Input Source:
    |  - External Hive Table: sales_data_external
    |  - Location: $externalTableLocation
    |
    |Transformations Applied:
    |  1. Extract customer analytics with enhanced metrics
    |  2. Extract product analytics with ranking
    |  3. Extract time-based analytics with trending
    |  4. Create unified asset view combining all dimensions
    |
    |Output:
    |  - Hive Table: sales_analytics_asset
    |  - Location: $assetTableLocation
    |  - Lineage Registry: Entry added to data_lineage_registry
    |  - Record Count: $assetRecordCount
    |==========================================================
  """.stripMargin)

} catch {
  case e: Exception => 
    println(s"ERROR: Asset creation job failed with exception:")
    println(s"${e.getMessage}")
    println(s"${e.getStackTrace.mkString("\n")}")
    
    // Log error in lineage format
    logLineage(
      "ERROR: Asset Creation Failed", 
      Seq(externalTableLocation), 
      s"Error: ${e.getMessage}"
    )
    
    throw e
}

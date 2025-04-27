// Spark Shell Script 2: Asset Creation from External Table
// Run with: spark-shell -i spark-shell-job2.scala

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

// Read from external table
println("Reading data from external Hive table...")
val salesDataDF = spark.read
  .format("parquet")
  .load(externalTableLocation)

// Show a quick sample to verify data loading
println("Sample data from external table:")
salesDataDF.show(5)

// Cache the data for better performance
salesDataDF.persist(StorageLevel.MEMORY_AND_DISK)

// Log lineage for data loading from external table
logLineage(
  "Load Data from External Table", 
  Seq(s"External Table: sales_data_external at $externalTableLocation"), 
  "salesDataDF"
)

// TRANSFORMATION 1: Extract customer analytics
val customerAnalyticsDF = salesDataDF
  .filter(col("customer_id").isNotNull) // Filter out summary rows
  .groupBy(col("customer_id"), col("customer_name"))
  .agg(
    count(col("order_id")).as("total_orders"),
    countDistinct(col("product_id")).as("unique_products_purchased"),
    sum(col("final_price")).as("total_spent"),
    max(col("order_date")).as("last_purchase_date"),
    min(col("order_date")).as("first_purchase_date"),
    avg(col("final_price") / col("quantity")).as("avg_unit_price"),
    sum(when(col("is_weekend") === true, col("final_price")).otherwise(0)).as("weekend_spending"),
    sum(when(col("is_weekend") === false, col("final_price")).otherwise(0)).as("weekday_spending")
  )
  
// Add calculated customer metrics
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
  .withColumn("weekend_shopper_ratio", col("weekend_spending") / (col("weekend_spending") + col("weekday_spending")))

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
val productAnalyticsDF = salesDataDF
  .filter(col("product_id").isNotNull) // Filter out summary rows
  .groupBy(col("product_id"), col("product_name"), col("category"))
  .agg(
    count(col("order_id")).as("order_count"),
    sum(col("quantity")).as("total_quantity_sold"),
    sum(col("final_price")).as("total_revenue"),
    avg(col("price")).as("avg_price"),
    avg(col("discount_amount") / col("extended_price")).as("avg_discount_rate"),
    countDistinct(col("customer_id")).as("unique_customers")
  )
  .withColumn("revenue_per_unit", col("total_revenue") / col("total_quantity_sold"))
  
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
val timeAnalyticsDF = salesDataDF
  .filter(col("order_date").isNotNull) // Filter out summary rows
  .withColumn("date", to_date(col("order_date")))
  .groupBy(col("date"), col("day_of_week"), col("month"), col("year"))
  .agg(
    count(col("order_id")).as("order_count"),
    countDistinct(col("customer_id")).as("unique_customers"),
    sum(col("final_price")).as("total_revenue"),
    avg(col("final_price")).as("avg_order_value"),
    sum(col("quantity")).as("items_sold"),
    countDistinct(col("product_id")).as("unique_products_sold")
  )
  .withColumn("revenue_per_customer", col("total_revenue") / col("unique_customers"))
  .withColumn("items_per_order", col("items_sold") / col("order_count"))
  
// Calculate moving averages for trending
val windowSpec7Day = Window
  .orderBy(col("date"))
  .rangeBetween(-6, 0) // 7-day window including current day
  
val windowSpec30Day = Window
  .orderBy(col("date"))
  .rangeBetween(-29, 0) // 30-day window including current day
  
val trendingTimeAnalyticsDF = timeAnalyticsDF
  .withColumn("revenue_7day_avg", avg(col("total_revenue")).over(windowSpec7Day))
  .withColumn("revenue_30day_avg", avg(col("total_revenue")).over(windowSpec30Day))
  .withColumn("order_count_7day_avg", avg(col("order_count")).over(windowSpec7Day))
  .withColumn("order_count_30day_avg", avg(col("order_count")).over(windowSpec30Day))
  .withColumn("is_revenue_trending_up", 
    col("revenue_7day_avg") > col("revenue_30day_avg") * 1.1 // 10% higher than 30-day average
  )

// Show time analytics
println("Sample time-based analytics with trending:")
trendingTimeAnalyticsDF.select("date", "total_revenue", "revenue_7day_avg", "revenue_30day_avg", "is_revenue_trending_up").show(5)

// Log lineage for time analytics transformation  
logLineage(
  "Extract Time-Based Analytics with Trends", 
  Seq("salesDataDF"), 
  "trendingTimeAnalyticsDF"
)

// TRANSFORMATION 4: Create assets by joining all analytics perspectives
// First, create a date dimension to join with
val today = LocalDate.now
val dateDf = (0 until 365).map(i => 
  (Date.valueOf(today.minusDays(i)), 
   today.minusDays(i).getDayOfWeek.toString,
   today.minusDays(i).getMonthValue,
   today.minusDays(i).getMonth.toString,
   today.minusDays(i).getYear)
).toDF("date", "day_of_week", "month_num", "month_name", "year")

// Prepare for joining to the date dimension
val timeAnalyticsForJoin = trendingTimeAnalyticsDF
  .withColumnRenamed("month", "month_name")
  .join(dateDf, Seq("date", "day_of_week", "month_name", "year"))

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

// Customer columns
val customerColumns = customerAssetDF.select(
  col("asset_type"), col("asset_id"), col("asset_name"), col("asset_created_date"), col("asset_job_id"),
  col("customer_id"), col("total_orders"), col("total_spent"), col("customer_value_score"), 
  col("first_purchase_date"), col("last_purchase_date"), col("days_since_last_purchase"),
  lit(null).cast("string").as("category"),
  lit(null).cast("int").as("revenue_rank_in_category"),
  lit(null).cast("timestamp").as("date"),
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
  lit(null).cast("timestamp").as("date"),
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

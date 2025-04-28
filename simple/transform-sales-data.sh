// Job 2: Transform Data and Create New Table (transform_sales_data.scala)
// Run with: spark-shell -i transform_sales_data.scala

import org.apache.spark.sql.functions._

println("=== Job 2: Transforming Sales Data ===")

// Define output path for the new table
val outputPath = "/data/warehouse/sales_analysis"

// Step 1: Read from the Hive table created in Job 1
println("Reading from sales_data Hive table...")

// Verify the table exists
if (!spark.catalog.tableExists("sales_data")) {
  throw new Exception("Table 'sales_data' does not exist. Please run Job 1 first.")
}

// Read the data
val salesDF = spark.table("sales_data")
println(s"Read ${salesDF.count()} records from sales_data")

// Display sample records
println("Sample data from sales_data:")
salesDF.show(5)

// Step 2: Apply transformations
println("Applying transformations...")

// Transformation 1: Calculate additional metrics
val transformedDF = salesDF
  // Add month name for better readability
  .withColumn("month_name", date_format(col("order_date"), "MMMM"))
  
  // Calculate extended_price from price and quantity
  .withColumn("extended_price", col("price") * col("quantity"))
  
  // Apply discount for bulk orders (> 2 items)
  .withColumn("discount", 
    when(col("quantity") > 2, col("extended_price") * 0.1)
    .otherwise(0.0)
  )
  
  // Calculate final price after discount
  .withColumn("final_price", col("extended_price") - col("discount"))
  
  // Add day of week
  .withColumn("day_of_week", date_format(col("order_date"), "EEEE"))
  
  // Flag weekend orders
  .withColumn("is_weekend", 
    date_format(col("order_date"), "u").isin("6", "7")
  )

// Transformation 2: Add product tier based on price
val withProductTier = transformedDF
  .withColumn("product_tier", 
    when(col("price") < 50, "Budget")
    .when(col("price") < 100, "Standard")
    .when(col("price") < 500, "Premium")
    .otherwise("Luxury")
  )

// Show the transformed data
println("Transformed data structure:")
withProductTier.printSchema()

println("Sample transformed data:")
withProductTier.select(
  "order_id", "customer_name", "product_name", 
  "quantity", "price", "extended_price", "discount", "final_price",
  "product_tier", "is_weekend", "month_name"
).show(5)

// Step 3: Create new Hive table with the transformed data
println("Creating new Hive table: sales_analysis...")

// Make sure the output directory is clean
val hdfs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
val outputDir = new org.apache.hadoop.fs.Path(outputPath)
if (hdfs.exists(outputDir)) {
  hdfs.delete(outputDir, true)
  println(s"Deleted existing directory: $outputPath")
}

// Save the transformed data
withProductTier.write
  .format("parquet")
  .mode("overwrite")
  .save(outputPath)

// Create Hive table
spark.sql("DROP TABLE IF EXISTS sales_analysis")
spark.sql(s"""
  CREATE TABLE sales_analysis
  USING PARQUET
  LOCATION '$outputPath'
""")

println("Hive table created: sales_analysis")

// Verify the table
println("Verifying table creation...")
spark.sql("SHOW TABLES").show()
println("Table content sample:")
spark.sql("SELECT * FROM sales_analysis LIMIT 5").show()

// Step 4: Run some simple analytics to demonstrate the new table
println("Running simple analytics on the transformed data...")

// Analytics 1: Revenue by product tier
println("Revenue by product tier:")
spark.sql("""
  SELECT 
    product_tier,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(final_price) AS total_revenue,
    AVG(final_price) AS avg_order_value
  FROM sales_analysis
  GROUP BY product_tier
  ORDER BY total_revenue DESC
""").show()

// Analytics 2: Weekend vs. Weekday sales
println("Weekend vs. Weekday sales:")
spark.sql("""
  SELECT 
    CASE WHEN is_weekend = true THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(final_price) AS total_revenue
  FROM sales_analysis
  GROUP BY is_weekend
""").show()

// Analytics 3: Monthly sales trend
println("Monthly sales:")
spark.sql("""
  SELECT 
    month_name,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(final_price) AS total_revenue
  FROM sales_analysis
  GROUP BY month_name
  ORDER BY month
""").show()

println("=== Job 2 Complete: Sales Analysis Table Created ===")
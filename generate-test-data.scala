// Updated Spark Shell Script 1: Data Processing for Sample Data
// Run with: spark-shell -i generate-test-data.scala

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import java.time.{LocalDate, LocalDateTime}
import java.sql.{Date, Timestamp}

// Function to log lineage information
def logLineage(description: String, inputs: Seq[String], output: String): Unit = {
  println(s"LINEAGE: $description")
  println(s"LINEAGE: Inputs: ${inputs.mkString(", ")}")
  println(s"LINEAGE: Output: $output")
  println(s"LINEAGE: Timestamp: ${java.time.LocalDateTime.now}")
  println("LINEAGE: " + "=" * 50)
}

println("Starting Data Processing Job from Spark Shell")

// Define paths for input files - these match what's created by generate-sample-data.sh
val customerFilePath = "/data/hdfs/customers.csv"
val ordersFilePath = "/data/hdfs/orders.csv"
val productsFilePath = "/data/hdfs/products.csv"

// External table location
val externalTableLocation = "/user/hive/external/processed_sales"

println(s"Reading from source files:")
println(s"- Customers: $customerFilePath")
println(s"- Orders: $ordersFilePath")
println(s"- Products: $productsFilePath")
println(s"Writing to external table at: $externalTableLocation")

// Load data directly with inferred schema for the sample data
println("Reading input files from HDFS...")

// Read customer data with header
val customersDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(customerFilePath)
  
// Read orders data with header
val ordersDF = spark.read
  .option("header", "true") 
  .option("inferSchema", "true")
  .csv(ordersFilePath)
  
// Read products data with header
val productsDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(productsFilePath)

// Log lineage for input data loading
logLineage(
  "Load Raw Data", 
  Seq(customerFilePath, ordersFilePath, productsFilePath), 
  "Raw DataFrames in memory"
)

// Display schemas to verify data was loaded correctly
println("Customer Data Schema:")
customersDF.printSchema()

println("Orders Data Schema:")
ordersDF.printSchema()

println("Products Data Schema:")
productsDF.printSchema()

// Show some sample data
println("Sample data from customers:")
customersDF.show(5)
println("Sample data from orders:")
ordersDF.show(5)
println("Sample data from products:")
productsDF.show(5)

// Cache intermediate DataFrames to improve performance
customersDF.persist(StorageLevel.MEMORY_AND_DISK)
ordersDF.persist(StorageLevel.MEMORY_AND_DISK)
productsDF.persist(StorageLevel.MEMORY_AND_DISK)

// TRANSFORMATION 1: Filter active customers only
val activeCustomersDF = customersDF.filter(col("status") === "active")
println(s"Active customers count: ${activeCustomersDF.count()}")
activeCustomersDF.show(5)

// TRANSFORMATION 2: Filter orders from last 90 days
val recentOrdersDF = ordersDF.filter(
  col("order_date") > date_sub(current_date(), 90)
)
println(s"Recent orders count: ${recentOrdersDF.count()}")
recentOrdersDF.show(5)

// TRANSFORMATION 3: Filter in-stock products only
val availableProductsDF = productsDF.filter(col("in_stock") === "true")
println(s"Available products count: ${availableProductsDF.count()}")
availableProductsDF.show(5)

// Log lineage for filtering transformations
logLineage(
  "Apply Filtering Transformations", 
  Seq("customersDF", "ordersDF", "productsDF"), 
  "Filtered DataFrames"
)

// TRANSFORMATION 4: Join data to create sales analysis
val salesAnalysisDF = recentOrdersDF
  .join(activeCustomersDF, "customer_id")
  .join(availableProductsDF, "product_id")
  .select(
    col("order_id"),
    col("customer_id"),
    col("name").as("customer_name"),
    col("email"),
    col("product_id"),
    availableProductsDF("name").as("product_name"),
    col("category"),
    col("price"),
    col("quantity"),
    col("order_date"),
    col("total_amount")
  )

println("Sample data after joins:")
salesAnalysisDF.show(5)

// Log lineage for join transformation
logLineage(
  "Join Data for Sales Analysis", 
  Seq("activeCustomersDF", "recentOrdersDF", "availableProductsDF"), 
  "salesAnalysisDF"
)

// TRANSFORMATION 5: Add derived columns
val enrichedSalesDF = salesAnalysisDF
  .withColumn("day_of_week", date_format(col("order_date"), "EEEE"))
  .withColumn("month", date_format(col("order_date"), "MMMM"))
  .withColumn("year", year(col("order_date")))
  .withColumn("is_weekend", when(
    date_format(col("order_date"), "u").isin("6", "7"), true
  ).otherwise(false))
  .withColumn("unit_price", col("price"))
  .withColumn("extended_price", col("price") * col("quantity"))
  .withColumn("discount_amount", when(col("quantity") > 10, col("extended_price") * 0.1).otherwise(0))
  .withColumn("final_price", col("extended_price") - col("discount_amount"))

// Show enriched data
println("Sample data after enrichment:")
enrichedSalesDF.select("order_id", "customer_name", "product_name", "day_of_week", "is_weekend", "final_price").show(5)

// Log lineage for enrichment transformation
logLineage(
  "Enrich Sales Data with Derived Columns", 
  Seq("salesAnalysisDF"), 
  "enrichedSalesDF"
)

// TRANSFORMATION 6: Calculate aggregations by product category
val categorySummaryDF = enrichedSalesDF
  .groupBy(col("category"))
  .agg(
    count(col("order_id")).as("order_count"),
    sum(col("quantity")).as("total_quantity"),
    sum(col("final_price")).as("total_sales"),
    avg(col("final_price")).as("avg_sale_value"),
    min(col("order_date")).as("first_order_date"),
    max(col("order_date")).as("last_order_date")
  )
  .orderBy(col("total_sales").desc)

// Show the category summary
println("Category summary:")
categorySummaryDF.show()

// Log lineage for aggregation transformation
logLineage(
  "Aggregate Sales Data by Category", 
  Seq("enrichedSalesDF"), 
  "categorySummaryDF"
)

// TRANSFORMATION 7: Union the detailed and summary data
// First, create a compatible schema for the summary data
val summaryWithCompatibleSchema = categorySummaryDF
  .withColumn("order_id", lit(null).cast(IntegerType))
  .withColumn("customer_id", lit(null).cast(IntegerType))
  .withColumn("customer_name", lit("SUMMARY").cast(StringType))
  .withColumn("email", lit(null).cast(StringType))
  .withColumn("product_id", lit(null).cast(IntegerType))
  .withColumn("product_name", lit("CATEGORY SUMMARY").cast(StringType))
  .withColumn("price", lit(null).cast(DoubleType))
  .withColumn("quantity", lit(null).cast(IntegerType))
  .withColumn("order_date", lit(null).cast(TimestampType))
  .withColumn("total_amount", lit(null).cast(DoubleType))
  .withColumn("day_of_week", lit(null).cast(StringType))
  .withColumn("month", lit(null).cast(StringType))
  .withColumn("year", lit(null).cast(IntegerType))
  .withColumn("is_weekend", lit(null).cast(BooleanType))
  .withColumn("unit_price", lit(null).cast(DoubleType))
  .withColumn("extended_price", lit(null).cast(DoubleType))
  .withColumn("discount_amount", lit(null).cast(DoubleType))
  .withColumn("final_price", lit(null).cast(DoubleType))
  .select(enrichedSalesDF.columns.map(col): _*)

// Combine detailed and summary data
val finalDataDF = enrichedSalesDF.union(summaryWithCompatibleSchema)
println("Final data sample (including summary rows):")
finalDataDF.show(5)

// Log lineage for final transformation
logLineage(
  "Create Final Dataset with Details and Summary", 
  Seq("enrichedSalesDF", "summaryWithCompatibleSchema"), 
  "finalDataDF"
)

// Create external Hive table
println("Creating external Hive table...")
spark.sql(s"DROP TABLE IF EXISTS sales_data_external")
spark.sql(
  s"""
    |CREATE EXTERNAL TABLE sales_data_external (
    |  order_id INT,
    |  customer_id INT,
    |  customer_name STRING,
    |  email STRING,
    |  product_id INT,
    |  product_name STRING,
    |  category STRING,
    |  price DOUBLE,
    |  quantity INT,
    |  order_date TIMESTAMP,
    |  total_amount DOUBLE,
    |  day_of_week STRING,
    |  month STRING,
    |  year INT,
    |  is_weekend BOOLEAN,
    |  unit_price DOUBLE,
    |  extended_price DOUBLE,
    |  discount_amount DOUBLE,
    |  final_price DOUBLE
    |)
    |STORED AS PARQUET
    |LOCATION '$externalTableLocation'
  """.stripMargin
)

// Write data to external table location
finalDataDF.write
  .mode("overwrite")
  .format("parquet")
  .save(externalTableLocation)

// Log lineage for writing to external table
logLineage(
  "Write Data to External Hive Table", 
  Seq("finalDataDF"), 
  s"Hive External Table: sales_data_external at $externalTableLocation"
)

// Verify the data was written correctly
val verifyTableDF = spark.sql("SELECT * FROM sales_data_external LIMIT 10")
println("Verifying data in external table:")
verifyTableDF.show()

// Count the records in the table
val recordCount = spark.sql("SELECT COUNT(*) FROM sales_data_external").first().getLong(0)
println(s"Total records in external table: $recordCount")

// Unpersist cached DataFrames
customersDF.unpersist()
ordersDF.unpersist()
productsDF.unpersist()

// Print completion message with lineage summary
println(s"""
  |==========================================================
  |JOB COMPLETE: Data Processing with Lineage Tracking
  |==========================================================
  |Input Sources:
  |  - $customerFilePath
  |  - $ordersFilePath
  |  - $productsFilePath
  |
  |Transformations Applied:
  |  1. Filter active customers
  |  2. Filter recent orders
  |  3. Filter available products
  |  4. Join data for sales analysis
  |  5. Enrich with derived columns
  |  6. Calculate category aggregations
  |  7. Combine detailed and summary data
  |
  |Output:
  |  - External Hive Table: sales_data_external
  |  - Location: $externalTableLocation
  |  - Record Count: $recordCount
  |==========================================================
""".stripMargin)

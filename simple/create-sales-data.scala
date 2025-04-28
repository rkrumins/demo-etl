// Job 1: Create Hive Table with Joins (create_sales_table.scala)
// Run with: spark-shell -i create_sales_table.scala

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

println("=== Job 1: Creating Sales Table with Joins ===")

// Define input and output paths
val rawDataPath = "/data/raw"
val outputPath = "/data/warehouse/sales_data"

// Step 1: Read data with explicit schemas
println("Reading source data...")

// Customer schema with renamed columns to avoid ambiguity
val customerSchema = StructType(Array(
  StructField("customer_id", IntegerType, false),
  StructField("customer_name", StringType, true),
  StructField("email", StringType, true),
  StructField("status", StringType, true)
))

// Product schema with renamed columns to avoid ambiguity
val productSchema = StructType(Array(
  StructField("product_id", IntegerType, false),
  StructField("product_name", StringType, true),
  StructField("category", StringType, true),
  StructField("price", DoubleType, true),
  StructField("in_stock", BooleanType, true)
))

// Order schema
val orderSchema = StructType(Array(
  StructField("order_id", IntegerType, false),
  StructField("customer_id", IntegerType, false),
  StructField("product_id", IntegerType, false),
  StructField("quantity", IntegerType, true),
  StructField("order_date", DateType, true),
  StructField("total_amount", DoubleType, true)
))

// Read CSV files with schemas
val customersDF = spark.read
  .option("header", "true")
  .schema(customerSchema)
  .csv(s"$rawDataPath/customers.csv")

val productsDF = spark.read
  .option("header", "true")
  .schema(productSchema)
  .csv(s"$rawDataPath/products.csv")

val ordersDF = spark.read
  .option("header", "true")
  .schema(orderSchema)
  .csv(s"$rawDataPath/orders.csv")

// Show data counts
println(s"Customers: ${customersDF.count()}")
println(s"Products: ${productsDF.count()}")
println(s"Orders: ${ordersDF.count()}")

// Step 2: Perform joins to create the sales data
println("Joining data to create sales information...")

// First join orders with customers
val ordersWithCustomers = ordersDF.join(
  customersDF,
  ordersDF("customer_id") === customersDF("customer_id"),
  "left"
)

// Then join with products
val salesData = ordersWithCustomers.join(
  productsDF,
  ordersWithCustomers("product_id") === productsDF("product_id"),
  "left"
)

// Step 3: Select relevant columns for the final table and add some derived columns
println("Creating final sales table...")

val finalSalesData = salesData.select(
  ordersDF("order_id"),
  ordersDF("customer_id"),
  customersDF("customer_name"),
  customersDF("email"),
  customersDF("status").as("customer_status"),
  ordersDF("product_id"),
  productsDF("product_name"),
  productsDF("category"),
  productsDF("price"),
  productsDF("in_stock"),
  ordersDF("quantity"),
  ordersDF("order_date"),
  ordersDF("total_amount")
)
.withColumn("month", date_format(col("order_date"), "MM"))
.withColumn("year", year(col("order_date")))
.withColumn("is_active_customer", when(col("customer_status") === "active", true).otherwise(false))

// Show the final data
println("Final sales data structure:")
finalSalesData.printSchema()

println("Sample sales data:")
finalSalesData.show(5)

// Step 4: Save as Hive table
println("Creating Hive table...")

// Make sure the output directory is clean
val hdfs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
val outputDir = new org.apache.hadoop.fs.Path(outputPath)
if (hdfs.exists(outputDir)) {
  hdfs.delete(outputDir, true)
  println(s"Deleted existing directory: $outputPath")
}

// Save the data
finalSalesData.write
  .format("parquet")
  .mode("overwrite")
  .save(outputPath)

// Create Hive table
spark.sql("DROP TABLE IF EXISTS sales_data")
spark.sql(s"""
  CREATE TABLE sales_data
  USING PARQUET
  LOCATION '$outputPath'
""")

println("Hive table created: sales_data")

// Verify the table
println("Verifying table creation...")
spark.sql("SHOW TABLES").show()
println("Table content sample:")
spark.sql("SELECT * FROM sales_data LIMIT 5").show()

println("=== Job 1 Complete: Sales Table Created ===")
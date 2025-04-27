// Fixed Spark Job 1: Data Processing (generate-test-data.scala)
// Run with: spark-shell -i generate-test-data.scala

// Import necessary libraries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Log lineage information
def logLineage(step: String, input: String, output: String): Unit = {
  println(s"LINEAGE: STEP=$step | INPUT=$input | OUTPUT=$output | TIME=${java.time.LocalDateTime.now}")
}

println("=== Starting Data Processing Job ===")

// Define paths
val inputDir = "/data/lineage"
val outputDir = "/data/lineage/processed"

try {
  // Step 1: Read source data with explicit schema to ensure correct data types for joins
  logLineage("READ_SOURCE", s"$inputDir/customers.csv,$inputDir/products.csv,$inputDir/orders.csv", "Raw DataFrames")
  
  // Define explicit schemas to ensure consistent types for joining
  val customerSchema = StructType(Array(
    StructField("customer_id", IntegerType, false),
    StructField("name", StringType, true),
    StructField("email", StringType, true),
    StructField("status", StringType, true)
  ))
  
  val productSchema = StructType(Array(
    StructField("product_id", IntegerType, false),
    StructField("name", StringType, true),
    StructField("category", StringType, true),
    StructField("price", DoubleType, true),
    StructField("in_stock", BooleanType, true)
  ))
  
  val orderSchema = StructType(Array(
    StructField("order_id", IntegerType, false),
    StructField("customer_id", IntegerType, false),
    StructField("product_id", IntegerType, false),
    StructField("quantity", IntegerType, true),
    StructField("order_date", DateType, true),
    StructField("total_amount", DoubleType, true)
  ))
  
  val customersDF = spark.read
    .option("header", "true")
    .schema(customerSchema)
    .csv(s"$inputDir/customers.csv")
  
  val productsDF = spark.read
    .option("header", "true")
    .schema(productSchema)
    .csv(s"$inputDir/products.csv")
  
  val ordersDF = spark.read
    .option("header", "true")
    .schema(orderSchema)
    .csv(s"$inputDir/orders.csv")
  
  // Verify data loading and schema
  println("Customers schema:")
  customersDF.printSchema()
  println("Customers sample:")
  customersDF.show(3)
  
  println("Products schema:")
  productsDF.printSchema()
  println("Products sample:")
  productsDF.show(3)
  
  println("Orders schema:")
  ordersDF.printSchema()
  println("Orders sample:")
  ordersDF.show(3)
  
  // Count records to verify data loading
  println(s"Customer count: ${customersDF.count()}")
  println(s"Product count: ${productsDF.count()}")
  println(s"Order count: ${ordersDF.count()}")
  
  // Step 2: Filter data
  logLineage("FILTER_DATA", "Raw DataFrames", "Filtered DataFrames")
  
  // Filter active customers - verify filter condition matches actual data
  val activeCustomersDF = customersDF.filter(col("status") === "active")
  println(s"Active customers: ${activeCustomersDF.count()}")
  activeCustomersDF.show(3)
  
  // Filter available products - verify filter condition matches actual data
  val availableProductsDF = productsDF.filter(col("in_stock") === true)
  println(s"Available products: ${availableProductsDF.count()}")
  availableProductsDF.show(3)
  
  // Filter recent orders - verify date parsing
  val recentOrdersDF = ordersDF.filter(
    col("order_date") > lit("2023-02-01") // Note: our schema ensures proper date type
  )
  println(s"Recent orders: ${recentOrdersDF.count()}")
  recentOrdersDF.show(3)
  
  // Step 3: Join data - this is the critical part for ensuring correct joins
  logLineage("JOIN_DATA", "Filtered DataFrames", "Sales DataFrame")
  
  // Print distinct IDs to see what should match in the join
  println("Distinct customer_id in active customers:")
  activeCustomersDF.select("customer_id").distinct().orderBy("customer_id").show(100)
  
  println("Distinct product_id in available products:")
  availableProductsDF.select("product_id").distinct().orderBy("product_id").show(100)
  
  println("Distinct customer_id and product_id in recent orders:")
  recentOrdersDF.select("customer_id", "product_id").distinct().orderBy("customer_id", "product_id").show(100)
  
  // First join orders with customers on customer_id
  val ordersWithCustomersDF = recentOrdersDF.join(
    activeCustomersDF,
    recentOrdersDF("customer_id") === activeCustomersDF("customer_id"),
    "inner"
  )
  
  println("After joining orders with customers:")
  println(s"Records: ${ordersWithCustomersDF.count()}")
  ordersWithCustomersDF.show(3)
  
  // Then join with products on product_id
  val salesDF = ordersWithCustomersDF.join(
    availableProductsDF,
    ordersWithCustomersDF("product_id") === availableProductsDF("product_id"),
    "inner"
  ).select(
    ordersWithCustomersDF("order_id"),
    ordersWithCustomersDF("customer_id"),
    activeCustomersDF("name").as("customer_name"),
    activeCustomersDF("email"),
    ordersWithCustomersDF("product_id"),
    availableProductsDF("name").as("product_name"),
    availableProductsDF("category"),
    availableProductsDF("price"),
    ordersWithCustomersDF("quantity"),
    ordersWithCustomersDF("order_date"),
    ordersWithCustomersDF("total_amount")
  )
  
  println("Joined sales data (final result of both joins):")
  println(s"Sales records: ${salesDF.count()}")
  salesDF.show(5)
  
  // Step 4: Add derived columns
  logLineage("ENHANCE_DATA", "Sales DataFrame", "Enhanced Sales DataFrame")
  
  val enhancedSalesDF = salesDF
    .withColumn("month", date_format(col("order_date"), "MMMM"))
    .withColumn("year", year(col("order_date")))
    .withColumn("unit_price", col("price"))
    .withColumn("extended_price", col("price") * col("quantity"))
  
  println("Enhanced sales data:")
  enhancedSalesDF.select("order_id", "customer_name", "product_name", "month", "extended_price").show(5)
  
  // Step 5: Write processed data
  logLineage("WRITE_PROCESSED", "Enhanced Sales DataFrame", s"$outputDir/sales")
  
  // Ensure output directory is clean
  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  val outputPath = new org.apache.hadoop.fs.Path(outputDir)
  if (hdfs.exists(outputPath)) {
    hdfs.delete(outputPath, true)
    println(s"Deleted existing directory: $outputDir")
  }
  
  // Write data
  enhancedSalesDF.write
    .mode("overwrite")
    .parquet(s"$outputDir/sales")
  
  println(s"Processed data written to: $outputDir/sales")
  
  // Verify written data by reading it back
  val verifyDF = spark.read.parquet(s"$outputDir/sales")
  println("Verification of written data:")
  println(s"Total records: ${verifyDF.count()}")
  verifyDF.show(3)
  
  // Create Hive table for easy access in Job 2
  spark.sql("DROP TABLE IF EXISTS processed_sales")
  spark.sql(s"""
    CREATE TABLE processed_sales
    USING PARQUET
    LOCATION '$outputDir/sales'
  """)
  
  println("Created Hive table: processed_sales")
  
  // Confirm table creation
  spark.sql("SHOW TABLES").show()
  spark.sql("SELECT * FROM processed_sales LIMIT 5").show()
  
  println("=== Data Processing Job Complete ===")

} catch {
  case e: Exception =>
    println(s"ERROR: ${e.getMessage}")
    e.printStackTrace()
}

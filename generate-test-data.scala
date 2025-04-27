// Spark Job 1: Fixed Column Ambiguity Issues (generate-test-data.scala)
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
  
  // Step 2: Filter data
  logLineage("FILTER_DATA", "Raw DataFrames", "Filtered DataFrames")
  
  // Rename columns that would be ambiguous in joins
  val customersDFRenamed = customersDF
    .withColumnRenamed("name", "customer_name")
  
  val productsDFRenamed = productsDF
    .withColumnRenamed("name", "product_name")
  
  // Filter active customers
  val activeCustomersDF = customersDFRenamed.filter(col("status") === "active")
  println(s"Active customers: ${activeCustomersDF.count()}")
  
  // Filter available products
  val availableProductsDF = productsDFRenamed.filter(col("in_stock") === true)
  println(s"Available products: ${availableProductsDF.count()}")
  
  // Filter recent orders
  val recentOrdersDF = ordersDF.filter(
    col("order_date") > lit("2023-02-01")
  )
  println(s"Recent orders: ${recentOrdersDF.count()}")
  
  // Step 3: Join data - using explicit join conditions and column selection
  logLineage("JOIN_DATA", "Filtered DataFrames", "Sales DataFrame")
  
  // Join orders with customers using explicit conditions
  val ordersWithCustomersDF = recentOrdersDF
    .join(
      activeCustomersDF,
      recentOrdersDF("customer_id") === activeCustomersDF("customer_id"),
      "inner"
    )
  
  // Join with products 
  val joinedDataDF = ordersWithCustomersDF
    .join(
      availableProductsDF,
      ordersWithCustomersDF("product_id") === availableProductsDF("product_id"),
      "inner"
    )
  
  // Select columns with fully qualified references to avoid ambiguity
  val salesDF = joinedDataDF.select(
    recentOrdersDF("order_id"),
    recentOrdersDF("customer_id"),
    activeCustomersDF("customer_name"),
    activeCustomersDF("email"),
    recentOrdersDF("product_id"),
    availableProductsDF("product_name"),
    availableProductsDF("category"),
    availableProductsDF("price"),
    recentOrdersDF("quantity"),
    recentOrdersDF("order_date"),
    recentOrdersDF("total_amount")
  )
  
  println("Joined sales data:")
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
  enhancedSalesDF.show(5)
  
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

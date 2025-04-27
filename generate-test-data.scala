// Generate Test Data and Push to HDFS
// Run with: spark-shell -i generate-test-data.scala

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.sql.{Date, Timestamp}
import java.util.UUID
import scala.util.Random

println("Starting Test Data Generation")

// Define paths for output files
val localOutputDir = "/tmp/spark-lineage-test-data"
val hdfsOutputDir = "/data/hdfs"

// Create local directory if it doesn't exist
import java.io.File
val localDir = new File(localOutputDir)
if (!localDir.exists()) {
  localDir.mkdirs()
  println(s"Created local directory: $localOutputDir")
}

// Function to create HDFS directory
def createHdfsDir(path: String): Unit = {
  import org.apache.hadoop.fs.{FileSystem, Path}
  import org.apache.hadoop.conf.Configuration
  
  val conf = new Configuration()
  val fs = FileSystem.get(conf)
  val hdfsPath = new Path(path)
  
  if (!fs.exists(hdfsPath)) {
    fs.mkdirs(hdfsPath)
    println(s"Created HDFS directory: $path")
  } else {
    println(s"HDFS directory already exists: $path")
  }
}

// Create HDFS directory
createHdfsDir(hdfsOutputDir)

// Helper function to generate random strings
def randomString(length: Int): String = {
  Random.alphanumeric.take(length).mkString
}

// Helper function to generate random email
def randomEmail(name: String): String = {
  val domains = Seq("gmail.com", "yahoo.com", "hotmail.com", "example.com", "company.com")
  val domain = domains(Random.nextInt(domains.size))
  s"${name.toLowerCase.replaceAll("[^a-z]", "")}.${randomString(4)}@$domain"
}

// Helper function for random dates within a range
def randomDate(startDate: LocalDate, endDate: LocalDate): Date = {
  val startEpochDay = startDate.toEpochDay
  val endEpochDay = endDate.toEpochDay
  val randomDay = startEpochDay + Random.nextInt((endEpochDay - startEpochDay).toInt + 1)
  Date.valueOf(LocalDate.ofEpochDay(randomDay))
}

// Helper function for random timestamps within a range
def randomTimestamp(startDate: LocalDate, endDate: LocalDate): Timestamp = {
  val randomDate = LocalDate.ofEpochDay(
    startDate.toEpochDay + Random.nextInt((endDate.toEpochDay - startDate.toEpochDay).toInt + 1)
  )
  val randomHour = Random.nextInt(24)
  val randomMinute = Random.nextInt(60)
  val randomSecond = Random.nextInt(60)
  
  val randomDateTime = randomDate.atTime(randomHour, randomMinute, randomSecond)
  Timestamp.valueOf(randomDateTime)
}

println("Generating customers data...")

// Generate customer data
val customerCount = 200
val customerData = (1 to customerCount).map { i =>
  val firstName = Seq("John", "Jane", "James", "Mary", "Robert", "Linda", "Michael", "Sarah", "David", "Jennifer")(Random.nextInt(10))
  val lastName = Seq("Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor")(Random.nextInt(10))
  val fullName = s"$firstName $lastName"
  val email = randomEmail(fullName)
  val signupDate = randomDate(LocalDate.now().minusYears(3), LocalDate.now().minusMonths(1))
  val status = if (Random.nextDouble() < 0.8) "active" else "inactive"
  
  (i, fullName, email, signupDate, status)
}

val customersDF = customerData.toDF("customer_id", "name", "email", "signup_date", "status")
println(s"Generated $customerCount customers")
println("Sample customers data:")
customersDF.show(5)

println("Generating products data...")

// Generate product data
val productCount = 50
val categories = Seq("Electronics", "Clothing", "Home Goods", "Books", "Toys", "Sports", "Food", "Beauty")

val productData = (1 to productCount).map { i =>
  val category = categories(Random.nextInt(categories.size))
  val productName = category match {
    case "Electronics" => Seq("Laptop", "Smartphone", "Tablet", "Headphones", "Camera")(Random.nextInt(5)) + " " + randomString(4)
    case "Clothing" => Seq("Shirt", "Pants", "Jacket", "Dress", "Shoes")(Random.nextInt(5)) + " " + randomString(4)
    case "Home Goods" => Seq("Pillow", "Lamp", "Chair", "Table", "Rug")(Random.nextInt(5)) + " " + randomString(4)
    case "Books" => Seq("Novel", "Biography", "Cookbook", "History", "Science")(Random.nextInt(5)) + " " + randomString(4)
    case "Toys" => Seq("Action Figure", "Board Game", "Doll", "Puzzle", "Lego")(Random.nextInt(5)) + " " + randomString(4)
    case "Sports" => Seq("Ball", "Racket", "Shoes", "Jersey", "Equipment")(Random.nextInt(5)) + " " + randomString(4)
    case "Food" => Seq("Snack", "Beverage", "Dessert", "Grain", "Protein")(Random.nextInt(5)) + " " + randomString(4)
    case "Beauty" => Seq("Lotion", "Makeup", "Perfume", "Shampoo", "Soap")(Random.nextInt(5)) + " " + randomString(4)
    case _ => "Product " + randomString(6)
  }
  
  val price = category match {
    case "Electronics" => 50.0 + Random.nextDouble() * 950.0
    case "Clothing" => 15.0 + Random.nextDouble() * 85.0
    case "Home Goods" => 20.0 + Random.nextDouble() * 480.0
    case "Books" => 5.0 + Random.nextDouble() * 25.0
    case "Toys" => 10.0 + Random.nextDouble() * 90.0
    case "Sports" => 15.0 + Random.nextDouble() * 185.0
    case "Food" => 2.0 + Random.nextDouble() * 18.0
    case "Beauty" => 5.0 + Random.nextDouble() * 45.0
    case _ => 10.0 + Random.nextDouble() * 50.0
  }
  
  val inStock = Random.nextDouble() < 0.85 // 85% of products are in stock
  
  (i, productName, category, math.round(price * 100) / 100.0, inStock)
}

val productsDF = productData.toDF("product_id", "name", "category", "price", "in_stock")
println(s"Generated $productCount products")
println("Sample products data:")
productsDF.show(5)

println("Generating orders data...")

// Generate order data
val orderCount = 2000
val today = LocalDate.now()
val threeYearsAgo = today.minusYears(3)

val orderData = (1 to orderCount).map { i =>
  val customerId = Random.nextInt(customerCount) + 1
  val productId = Random.nextInt(productCount) + 1
  val quantity = 1 + Random.nextInt(10) // Order between 1-10 items
  
  // Recent orders more likely
  val orderDate = if (Random.nextDouble() < 0.4) {
    // 40% of orders in last 90 days
    randomTimestamp(today.minusDays(90), today)
  } else {
    // 60% of orders from 3 years ago to 90 days ago
    randomTimestamp(threeYearsAgo, today.minusDays(91))
  }
  
  // Look up the product price
  val product = productData(productId - 1)
  val price = product._4
  val totalAmount = price * quantity
  
  (i, customerId, productId, quantity, orderDate, math.round(totalAmount * 100) / 100.0)
}

val ordersDF = orderData.toDF("order_id", "customer_id", "product_id", "quantity", "order_date", "total_amount")
println(s"Generated $orderCount orders")
println("Sample orders data:")
ordersDF.show(5)

// Write all DataFrames to local CSV files
println("Writing data to local CSV files...")

customersDF.write
  .option("header", "true")
  .mode("overwrite")
  .csv(s"$localOutputDir/customers.csv")

productsDF.write
  .option("header", "true")
  .mode("overwrite")
  .csv(s"$localOutputDir/products.csv")

ordersDF.write
  .option("header", "true")
  .mode("overwrite")
  .csv(s"$localOutputDir/orders.csv")

// Function to copy a single file from a directory of part files
def copyLocalToHdfs(localDir: String, fileName: String, hdfsDir: String): Unit = {
  import scala.sys.process._
  
  // First, create a single file from the part-* files
  val localPartFiles = new File(localDir).listFiles().filter(_.getName.startsWith("part-"))
  val singleFile = s"$localOutputDir/$fileName"
  
  // Get the header from the part file
  val header = s"cat $localDir/part-00000-*.csv | head -n 1".!!
  
  // Write header to the output file
  s"echo '$header' > $singleFile".!
  
  // Append all data rows (skipping headers) to the output file
  localPartFiles.foreach { file =>
    s"cat ${file.getAbsolutePath} | tail -n +2 >> $singleFile".!
  }
  
  // Now copy the single file to HDFS
  s"hdfs dfs -put -f $singleFile $hdfsDir/$fileName".!
  println(s"Copied $fileName to HDFS at $hdfsDir/$fileName")
}

// Now copy the generated data to HDFS
println("Copying data to HDFS...")

// Copy CSV files to HDFS
def copyToHdfs(inputDir: String, fileName: String, hdfsDir: String): Unit = {
  // Create a temporary single file from part files
  val tempFile = s"/tmp/$fileName"
  val localPartDir = s"$inputDir/$fileName"
  
  // Create a collection of DataFrames from local files
  val df = spark.read
    .option("header", "true")
    .csv(localPartDir)
  
  // Write as a single CSV file
  df.coalesce(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv(s"$inputDir/single_$fileName")
  
  // Find the part file
  val partFile = new File(s"$inputDir/single_$fileName").listFiles()
    .filter(f => f.getName.startsWith("part-") && f.getName.endsWith(".csv"))
    .head
  
  // Copy to temp location
  import java.nio.file.{Files, Paths, StandardCopyOption}
  Files.copy(
    Paths.get(partFile.getAbsolutePath), 
    Paths.get(tempFile), 
    StandardCopyOption.REPLACE_EXISTING
  )
  
  // Copy to HDFS
  import org.apache.hadoop.fs.{FileSystem, Path}
  import org.apache.hadoop.conf.Configuration
  
  val conf = new Configuration()
  val fs = FileSystem.get(conf)
  
  fs.copyFromLocalFile(
    true, // delete source
    true, // overwrite destination
    new Path(tempFile),
    new Path(s"$hdfsDir/$fileName")
  )
  
  println(s"Copied $fileName to HDFS at $hdfsDir/$fileName")
}

// Copy the data files to HDFS
try {
  copyToHdfs(localOutputDir, "customers.csv", hdfsOutputDir)
  copyToHdfs(localOutputDir, "products.csv", hdfsOutputDir)
  copyToHdfs(localOutputDir, "orders.csv", hdfsOutputDir)
  println("All files successfully copied to HDFS")
} catch {
  case e: Exception => 
    println(s"Error copying files to HDFS: ${e.getMessage}")
    println("Falling back to local HDFS command...")
    
    // Alternative approach using HDFS shell commands
    import scala.sys.process._
    val hdfsCmd = "hdfs dfs -put -f"
    
    // Create single files from the part files
    val customerFile = s"$localOutputDir/customers_single.csv"
    val productFile = s"$localOutputDir/products_single.csv"
    val orderFile = s"$localOutputDir/orders_single.csv"
    
    // Read and write as single files
    customersDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(customerFile)
    productsDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(productFile)
    ordersDF.coalesce(1).write.option("header", "true").mode("overwrite").csv(orderFile)
    
    // Find the part files
    def findPartFile(dir: String): String = {
      new java.io.File(dir).listFiles()
        .filter(f => f.getName.startsWith("part-") && f.getName.endsWith(".csv"))
        .head.getAbsolutePath
    }
    
    // Copy to HDFS
    s"$hdfsCmd ${findPartFile(customerFile)} $hdfsOutputDir/customers.csv".!
    s"$hdfsCmd ${findPartFile(productFile)} $hdfsOutputDir/products.csv".!
    s"$hdfsCmd ${findPartFile(orderFile)} $hdfsOutputDir/orders.csv".!
    
    println("Files copied to HDFS using shell commands")
}

// Alternative simple approach - just write directly to HDFS
println("Writing data directly to HDFS (backup method)...")

customersDF.coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .csv(s"$hdfsOutputDir/customers_direct.csv")

productsDF.coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .csv(s"$hdfsOutputDir/products_direct.csv")

ordersDF.coalesce(1)
  .write
  .option("header", "true")
  .mode("overwrite")
  .csv(s"$hdfsOutputDir/orders_direct.csv")

// Verify data is in HDFS
println("Verifying data in HDFS...")

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

val conf = new Configuration()
val fs = FileSystem.get(conf)

def checkHdfsFile(path: String): Unit = {
  val hdfsPath = new Path(path)
  if (fs.exists(hdfsPath)) {
    val status = fs.getFileStatus(hdfsPath)
    println(s"File exists in HDFS: $path (Size: ${status.getLen} bytes)")
  } else {
    println(s"File does not exist in HDFS: $path")
  }
}

// Check regular files
checkHdfsFile(s"$hdfsOutputDir/customers.csv")
checkHdfsFile(s"$hdfsOutputDir/products.csv")
checkHdfsFile(s"$hdfsOutputDir/orders.csv")

// Check direct files (as backup)
checkHdfsFile(s"$hdfsOutputDir/customers_direct.csv")
checkHdfsFile(s"$hdfsOutputDir/products_direct.csv")
checkHdfsFile(s"$hdfsOutputDir/orders_direct.csv")

println("Test data generation complete!")
println(s"""
  |==========================================================
  |SUMMARY
  |==========================================================
  |Generated Data:
  |  - $customerCount customers
  |  - $productCount products
  |  - $orderCount orders
  |
  |Local Location:
  |  - $localOutputDir
  |
  |HDFS Location:
  |  - $hdfsOutputDir
  |==========================================================
""".stripMargin)

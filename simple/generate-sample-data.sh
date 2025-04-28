#!/bin/bash
# Simple Data Generator (generate-sample-data.sh)
# Creates basic CSV files and uploads them to HDFS

echo "=== Generating Sample Data ==="

# Create local directory for data
mkdir -p ./data

# Generate customers.csv with 10 records
echo "Generating customers.csv..."
cat > ./data/customers.csv << EOF
customer_id,customer_name,email,status
1,John Smith,john@example.com,active
2,Jane Doe,jane@example.com,active
3,Bob Johnson,bob@example.com,inactive
4,Mary Williams,mary@example.com,active
5,James Brown,james@example.com,active
6,Patricia Davis,patricia@example.com,inactive
7,Robert Miller,robert@example.com,active
8,Linda Wilson,linda@example.com,active
9,Michael Moore,michael@example.com,active
10,Elizabeth Taylor,elizabeth@example.com,inactive
EOF

# Generate products.csv with 10 records
echo "Generating products.csv..."
cat > ./data/products.csv << EOF
product_id,product_name,category,price,in_stock
1,Laptop,Electronics,999.99,true
2,Smartphone,Electronics,599.99,true
3,Headphones,Electronics,99.99,true
4,T-shirt,Clothing,19.99,true
5,Jeans,Clothing,49.99,false
6,Coffee Maker,Home,89.99,true
7,Blender,Home,69.99,false
8,Book,Books,14.99,true
9,Tablet,Electronics,299.99,true
10,Watch,Accessories,159.99,true
EOF

# Generate orders.csv with 20 records
echo "Generating orders.csv..."
cat > ./data/orders.csv << EOF
order_id,customer_id,product_id,quantity,order_date,total_amount
1,1,2,1,2023-01-15,599.99
2,2,1,1,2023-01-16,999.99
3,4,3,2,2023-01-20,199.98
4,5,8,3,2023-01-25,44.97
5,7,4,2,2023-01-30,39.98
6,8,6,1,2023-02-02,89.99
7,1,9,1,2023-02-05,299.99
8,4,10,1,2023-02-10,159.99
9,5,2,1,2023-02-15,599.99
10,7,1,1,2023-02-20,999.99
11,2,3,1,2023-02-25,99.99
12,8,8,2,2023-03-01,29.98
13,9,6,1,2023-03-05,89.99
14,1,4,3,2023-03-10,59.97
15,4,9,1,2023-03-15,299.99
16,5,10,1,2023-03-20,159.99
17,7,2,1,2023-03-25,599.99
18,8,3,2,2023-03-30,199.98
19,9,1,1,2023-04-05,999.99
20,2,4,2,2023-04-10,39.98
EOF

# Create HDFS directory if it doesn't exist
echo "Creating HDFS directory..."
hdfs dfs -mkdir -p /data/raw

# Upload files to HDFS
echo "Uploading files to HDFS..."
hdfs dfs -put -f ./data/customers.csv /data/raw/
hdfs dfs -put -f ./data/products.csv /data/raw/
hdfs dfs -put -f ./data/orders.csv /data/raw/

# Verify files in HDFS
echo "Verifying files in HDFS..."
hdfs dfs -ls /data/raw/

echo "=== Sample Data Generation Complete ==="
echo "Files available in HDFS at /data/raw/"
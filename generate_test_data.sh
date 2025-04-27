#!/bin/bash
# generate-sample-data.sh
# This script creates sample data files and pushes them to HDFS

# Set up variables
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
LOCAL_DATA_DIR="${SCRIPT_DIR}/data"
HDFS_DATA_DIR="/data/hdfs"
LOG_FILE="${SCRIPT_DIR}/data_generation.log"

# Create local data directory if it doesn't exist
mkdir -p "${LOCAL_DATA_DIR}"

echo "=========================================="
echo "Starting Sample Data Generation"
echo "=========================================="
echo "$(date): Starting sample data generation" > "${LOG_FILE}"

# Function to check if a command exists
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# Check if HDFS commands are available
if ! command_exists hdfs; then
  echo "Error: HDFS command not found. Please ensure Hadoop is properly installed."
  exit 1
fi

# Check if the HDFS directory exists, if not create it
hdfs dfs -test -e "${HDFS_DATA_DIR}" || hdfs dfs -mkdir -p "${HDFS_DATA_DIR}"
if [ $? -ne 0 ]; then
  echo "Error: Could not create HDFS directory ${HDFS_DATA_DIR}"
  exit 1
fi

# ----------------------------------------
# Generate Customers Data
# ----------------------------------------
echo "Generating customers data..."
echo "$(date): Generating customers data" >> "${LOG_FILE}"

cat > "${LOCAL_DATA_DIR}/customers.csv" << EOF
customer_id,name,email,signup_date,status
1,John Smith,john.smith@example.com,2022-05-10,active
2,Jane Johnson,jane.johnson@example.com,2022-06-15,active
3,Robert Williams,robert.williams@example.com,2022-04-22,inactive
4,Mary Brown,mary.brown@example.com,2022-07-05,active
5,James Davis,james.davis@example.com,2022-03-18,active
6,Jennifer Miller,jennifer.miller@example.com,2022-08-30,active
7,Michael Wilson,michael.wilson@example.com,2022-02-14,inactive
8,Linda Moore,linda.moore@example.com,2022-09-01,active
9,David Taylor,david.taylor@example.com,2022-01-20,active
10,Patricia Anderson,patricia.anderson@example.com,2022-10-05,active
11,Richard Thomas,richard.thomas@example.com,2021-11-15,inactive
12,Barbara Jackson,barbara.jackson@example.com,2022-10-12,active
13,Charles White,charles.white@example.com,2021-12-23,active
14,Susan Harris,susan.harris@example.com,2022-11-05,active
15,Joseph Martin,joseph.martin@example.com,2021-10-30,active
16,Margaret Thompson,margaret.thompson@example.com,2022-12-01,active
17,Thomas Garcia,thomas.garcia@example.com,2021-09-22,inactive
18,Dorothy Martinez,dorothy.martinez@example.com,2023-01-10,active
19,Christopher Robinson,christopher.robinson@example.com,2021-08-18,active
20,Elizabeth Clark,elizabeth.clark@example.com,2023-02-05,active
EOF

echo "Created customers.csv with 20 records"

# ----------------------------------------
# Generate Products Data
# ----------------------------------------
echo "Generating products data..."
echo "$(date): Generating products data" >> "${LOG_FILE}"

cat > "${LOCAL_DATA_DIR}/products.csv" << EOF
product_id,name,category,price,in_stock
1,Smartphone X,Electronics,699.99,true
2,Laptop Pro,Electronics,1299.99,true
3,Wireless Headphones,Electronics,149.99,true
4,Smart Watch,Electronics,249.99,true
5,Tablet Mini,Electronics,399.99,false
6,Cotton T-Shirt,Clothing,19.99,true
7,Denim Jeans,Clothing,49.99,true
8,Winter Jacket,Clothing,89.99,true
9,Running Shoes,Clothing,79.99,false
10,Summer Dress,Clothing,59.99,true
11,Coffee Table,Home Goods,199.99,true
12,Bed Frame,Home Goods,299.99,true
13,Standing Lamp,Home Goods,89.99,true
14,Kitchen Blender,Home Goods,79.99,false
15,Dining Chair Set,Home Goods,249.99,true
16,Mystery Novel,Books,14.99,true
17,Cookbook Collection,Books,29.99,true
18,History Textbook,Books,49.99,true
19,Science Fiction Series,Books,39.99,false
20,Biography Collection,Books,24.99,true
21,Action Figure Set,Toys,34.99,true
22,Board Game Classic,Toys,29.99,true
23,Building Blocks,Toys,49.99,true
24,Remote Control Car,Toys,59.99,false
25,Puzzle Collection,Toys,19.99,true
EOF

echo "Created products.csv with 25 records"

# ----------------------------------------
# Generate Orders Data
# ----------------------------------------
echo "Generating orders data..."
echo "$(date): Generating orders data" >> "${LOG_FILE}"

cat > "${LOCAL_DATA_DIR}/orders.csv" << EOF
order_id,customer_id,product_id,quantity,order_date,total_amount
1,5,12,1,2023-01-15 14:30:45,299.99
2,3,7,2,2023-01-17 09:15:22,99.98
3,8,1,1,2023-01-18 18:45:10,699.99
4,12,15,1,2023-01-20 11:22:33,249.99
5,6,3,1,2023-01-22 15:10:05,149.99
6,15,8,1,2023-01-25 16:40:18,89.99
7,2,22,2,2023-01-27 10:05:40,59.98
8,9,6,3,2023-01-30 13:25:52,59.97
9,18,11,1,2023-02-02 12:10:15,199.99
10,1,17,1,2023-02-05 17:30:20,29.99
11,14,4,1,2023-02-08 09:45:33,249.99
12,7,21,2,2023-02-10 14:20:45,69.98
13,20,2,1,2023-02-12 16:15:10,1299.99
14,11,19,1,2023-02-15 11:05:22,39.99
15,4,9,1,2023-02-18 15:40:30,79.99
16,16,14,1,2023-02-20 10:25:15,79.99
17,10,23,1,2023-02-22 13:15:50,49.99
18,19,1,1,2023-02-25 16:30:05,699.99
19,13,16,2,2023-02-28 09:55:15,29.98
20,17,10,1,2023-03-02 12:45:33,59.99
21,5,3,1,2023-03-05 14:20:18,149.99
22,8,7,1,2023-03-08 11:10:25,49.99
23,2,11,1,2023-03-10 16:35:42,199.99
24,15,1,1,2023-03-12 10:05:15,699.99
25,6,22,2,2023-03-15 13:40:30,59.98
26,12,4,1,2023-03-18 15:25:10,249.99
27,3,21,1,2023-03-20 09:15:45,34.99
28,9,6,2,2023-03-22 12:30:22,39.98
29,1,12,1,2023-03-25 16:45:10,299.99
30,18,3,1,2023-03-28 11:20:35,149.99
31,14,8,1,2023-04-01 14:10:20,89.99
32,20,17,1,2023-04-03 10:25:15,29.99
33,7,2,1,2023-04-05 13:35:42,1299.99
34,16,23,1,2023-04-08 16:15:10,49.99
35,4,16,2,2023-04-10 09:40:25,29.98
36,10,1,1,2023-04-12 12:20:18,699.99
37,19,14,1,2023-04-15 15:10:30,79.99
38,13,22,2,2023-04-18 11:45:15,59.98
39,5,11,1,2023-04-20 14:30:22,199.99
40,11,6,3,2023-04-22 10:05:40,59.97
41,8,16,1,2023-04-25 13:15:55,14.99
42,1,3,1,2023-04-28 16:40:10,149.99
43,12,8,1,2023-05-01 09:25:30,89.99
44,6,23,1,2023-05-03 12:10:45,49.99
45,3,1,1,2023-05-05 15:30:20,699.99
46,15,7,2,2023-05-08 11:15:35,99.98
47,9,12,1,2023-05-10 14:25:50,299.99
48,18,4,1,2023-05-12 10:45:15,249.99
49,2,17,1,2023-05-15 13:20:30,29.99
50,14,2,1,2023-05-18 16:05:45,1299.99
51,5,16,1,2023-05-20 09:30:10,14.99
52,11,6,2,2023-05-22 12:45:25,39.98
53,20,9,1,2023-05-25 15:10:40,79.99
54,7,21,2,2023-05-28 11:35:15,69.98
55,16,3,1,2023-05-30 14:20:30,149.99
56,4,1,1,2023-06-02 10:05:45,699.99
57,13,11,1,2023-06-05 13:30:20,199.99
58,19,8,1,2023-06-08 16:15:35,89.99
59,10,22,2,2023-06-10 09:40:50,59.98
60,1,15,1,2023-06-12 12:25:15,249.99
61,8,3,1,2023-06-15 15:10:30,149.99
62,15,7,1,2023-06-18 11:45:45,49.99
63,6,1,1,2023-06-20 14:30:10,699.99
64,12,16,2,2023-06-22 10:15:25,29.98
65,3,11,1,2023-06-25 13:40:40,199.99
66,9,4,1,2023-06-28 16:20:15,249.99
67,18,23,1,2023-07-01 09:05:30,49.99
68,2,6,3,2023-07-03 12:30:45,59.97
69,14,12,1,2023-07-05 15:15:10,299.99
70,5,8,1,2023-07-08 11:40:25,89.99
71,20,1,1,2023-07-10 14:25:40,699.99
72,7,17,1,2023-07-12 10:10:15,29.99
73,16,2,1,2023-07-15 13:35:30,1299.99
74,4,21,2,2023-07-18 16:20:45,69.98
75,10,6,2,2023-07-20 09:45:10,39.98
76,19,11,1,2023-07-22 12:30:25,199.99
77,13,3,1,2023-07-25 15:15:40,149.99
78,1,8,1,2023-07-28 11:40:15,89.99
79,12,23,1,2023-07-30 14:25:30,49.99
80,6,16,2,2023-08-02 10:05:45,29.98
81,3,1,1,2023-08-05 13:30:10,699.99
82,15,7,1,2023-08-08 16:15:25,49.99
83,9,12,1,2023-08-10 09:40:40,299.99
84,8,4,1,2023-08-12 12:25:15,249.99
85,2,22,2,2023-08-15 15:10:30,59.98
86,14,6,3,2023-08-18 11:45:45,59.97
87,5,2,1,2023-08-20 14:30:10,1299.99
88,11,17,1,2023-08-22 10:15:25,29.99
89,20,3,1,2023-08-25 13:40:40,149.99
90,7,8,1,2023-08-28 16:20:15,89.99
91,16,11,1,2023-08-30 09:05:30,199.99
92,4,23,1,2023-09-02 12:30:45,49.99
93,10,1,1,2023-09-05 15:15:10,699.99
94,19,16,2,2023-09-08 11:40:25,29.98
95,13,7,2,2023-09-10 14:25:40,99.98
96,1,12,1,2023-09-12 10:10:15,299.99
97,18,3,1,2023-09-15 13:35:30,149.99
98,6,8,1,2023-09-18 16:20:45,89.99
99,3,21,2,2023-09-20 09:45:10,69.98
100,9,6,3,2023-09-22 12:30:25,59.97
EOF

echo "Created orders.csv with 100 records"

# ----------------------------------------
# Upload files to HDFS
# ----------------------------------------
echo "Uploading files to HDFS..."
echo "$(date): Uploading files to HDFS" >> "${LOG_FILE}"

# Upload customers.csv to HDFS
hdfs dfs -put -f "${LOCAL_DATA_DIR}/customers.csv" "${HDFS_DATA_DIR}/"
if [ $? -ne 0 ]; then
  echo "Error: Failed to upload customers.csv to HDFS"
  echo "$(date): Failed to upload customers.csv to HDFS" >> "${LOG_FILE}"
  exit 1
else
  echo "Successfully uploaded customers.csv to HDFS"
  echo "$(date): Successfully uploaded customers.csv to HDFS" >> "${LOG_FILE}"
fi

# Upload products.csv to HDFS
hdfs dfs -put -f "${LOCAL_DATA_DIR}/products.csv" "${HDFS_DATA_DIR}/"
if [ $? -ne 0 ]; then
  echo "Error: Failed to upload products.csv to HDFS"
  echo "$(date): Failed to upload products.csv to HDFS" >> "${LOG_FILE}"
  exit 1
else
  echo "Successfully uploaded products.csv to HDFS"
  echo "$(date): Successfully uploaded products.csv to HDFS" >> "${LOG_FILE}"
fi

# Upload orders.csv to HDFS
hdfs dfs -put -f "${LOCAL_DATA_DIR}/orders.csv" "${HDFS_DATA_DIR}/"
if [ $? -ne 0 ]; then
  echo "Error: Failed to upload orders.csv to HDFS"
  echo "$(date): Failed to upload orders.csv to HDFS" >> "${LOG_FILE}"
  exit 1
else
  echo "Successfully uploaded orders.csv to HDFS"
  echo "$(date): Successfully uploaded orders.csv to HDFS" >> "${LOG_FILE}"
fi

# ----------------------------------------
# Verify files in HDFS
# ----------------------------------------
echo "Verifying files in HDFS..."
echo "$(date): Verifying files in HDFS" >> "${LOG_FILE}"

# List files in HDFS directory
echo "Files in HDFS directory ${HDFS_DATA_DIR}:"
hdfs dfs -ls "${HDFS_DATA_DIR}"

# Check file sizes
echo "File sizes in HDFS:"
hdfs dfs -du -h "${HDFS_DATA_DIR}"

# Sample the data to verify
echo "Sample of customers.csv from HDFS:"
hdfs dfs -cat "${HDFS_DATA_DIR}/customers.csv" | head -5

echo "Sample of products.csv from HDFS:"
hdfs dfs -cat "${HDFS_DATA_DIR}/products.csv" | head -5

echo "Sample of orders.csv from HDFS:"
hdfs dfs -cat "${HDFS_DATA_DIR}/orders.csv" | head -5

echo "=========================================="
echo "Sample Data Generation Complete"
echo "=========================================="
echo "$(date): Sample data generation complete" >> "${LOG_FILE}"
echo ""
echo "Data files created:"
echo "  - ${LOCAL_DATA_DIR}/customers.csv (20 records)"
echo "  - ${LOCAL_DATA_DIR}/products.csv (25 records)"
echo "  - ${LOCAL_DATA_DIR}/orders.csv (100 records)"
echo ""
echo "Data files uploaded to HDFS:"
echo "  - ${HDFS_DATA_DIR}/customers.csv"
echo "  - ${HDFS_DATA_DIR}/products.csv"
echo "  - ${HDFS_DATA_DIR}/orders.csv"
echo ""
echo "Log file: ${LOG_FILE}"
echo "=========================================="

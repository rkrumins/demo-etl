#!/bin/bash
# Spark Shell Workflow Script
# This script orchestrates the execution of all Spark shell scripts

echo "===================================================================="
echo "SPARK WORKFLOW WITH LINEAGE TRACKING (SPARK SHELL VERSION)"
echo "===================================================================="

# Set environment variables
export SPARK_HOME=${SPARK_HOME:-/usr/lib/spark}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}
export WORKFLOW_ID=$(uuidgen || echo "workflow-$(date +%s)")
export WORKFLOW_START_TIME=$(date '+%Y-%m-%d %H:%M:%S')

# Paths
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
GENERATE_SCRIPT="$SCRIPT_DIR/generate-test-data.scala"
JOB1_SCRIPT="$SCRIPT_DIR/spark-shell-job1.scala"
JOB2_SCRIPT="$SCRIPT_DIR/spark-shell-job2.scala"
LOG_DIR="$SCRIPT_DIR/logs"
HDFS_INPUT_DIR="/data/hdfs"
HIVE_EXTERNAL_TABLE_PATH="/user/hive/external/processed_sales"
HIVE_ASSET_TABLE_PATH="/user/hive/warehouse/sales_analytics_asset"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log workflow start
echo "Workflow ID: $WORKFLOW_ID" | tee "$LOG_DIR/workflow_$WORKFLOW_ID.log"
echo "Start Time: $WORKFLOW_START_TIME" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"

# Function to check if a script exists
check_script() {
    if [ ! -f "$1" ]; then
        echo "ERROR: Script not found: $1" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
        exit 1
    fi
}

# Check if scripts exist
check_script "$GENERATE_SCRIPT"
check_script "$JOB1_SCRIPT"
check_script "$JOB2_SCRIPT"

# Step 1: Generate test data and push to HDFS
echo "===================================================================="
echo "STEP 1: GENERATING TEST DATA AND PUSHING TO HDFS" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
echo "Start Time: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
echo "===================================================================="

$SPARK_HOME/bin/spark-shell \
    --master yarn \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf spark.workflow.id=$WORKFLOW_ID \
    -i "$GENERATE_SCRIPT" 2>&1 | tee "$LOG_DIR/generate_$WORKFLOW_ID.log"

# Check if generation failed
if [ $? -ne 0 ]; then
    echo "ERROR: Data generation failed! See logs for details." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
    exit 1
fi

# Verify that files were created in HDFS
echo "Verifying files in HDFS..." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
hdfs dfs -ls $HDFS_INPUT_DIR | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"

# Make sure the primary or direct files exist
if hdfs dfs -ls $HDFS_INPUT_DIR/customers.csv 2>/dev/null || hdfs dfs -ls $HDFS_INPUT_DIR/customers_direct.csv 2>/dev/null; then
    echo "Customers data found in HDFS." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
else
    echo "ERROR: Customers data not found in HDFS!" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
    exit 1
fi

if hdfs dfs -ls $HDFS_INPUT_DIR/products.csv 2>/dev/null || hdfs dfs -ls $HDFS_INPUT_DIR/products_direct.csv 2>/dev/null; then
    echo "Products data found in HDFS." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
else
    echo "ERROR: Products data not found in HDFS!" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
    exit 1
fi

if hdfs dfs -ls $HDFS_INPUT_DIR/orders.csv 2>/dev/null || hdfs dfs -ls $HDFS_INPUT_DIR/orders_direct.csv 2>/dev/null; then
    echo "Orders data found in HDFS." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
else
    echo "ERROR: Orders data not found in HDFS!" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
    exit 1
fi

# Step 2: Run first Spark job: Data Processing & External Table Creation
echo "===================================================================="
echo "STEP 2: RUNNING JOB 1: Data Processing with Lineage Tracking" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
echo "Start Time: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
echo "===================================================================="

# If we're using direct files, update the script paths
if ! hdfs dfs -ls $HDFS_INPUT_DIR/customers.csv >/dev/null 2>&1; then
    echo "Using direct files for processing..." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
    
    # Create a temporary script with updated paths
    TMP_JOB1_SCRIPT="$LOG_DIR/job1_tmp_$WORKFLOW_ID.scala"
    cp "$JOB1_SCRIPT" "$TMP_JOB1_SCRIPT"
    
    # Update the file paths in the script
    sed -i "s|/data/hdfs/customers.csv|/data/hdfs/customers_direct.csv|g" "$TMP_JOB1_SCRIPT"
    sed -i "s|/data/hdfs/products.csv|/data/hdfs/products_direct.csv|g" "$TMP_JOB1_SCRIPT"
    sed -i "s|/data/hdfs/orders.csv|/data/hdfs/orders_direct.csv|g" "$TMP_JOB1_SCRIPT"
    
    JOB1_SCRIPT="$TMP_JOB1_SCRIPT"
    echo "Updated file paths in job script" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
fi

$SPARK_HOME/bin/spark-shell \
    --master yarn \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf spark.workflow.id=$WORKFLOW_ID \
    --conf spark.sql.warehouse.dir=/user/hive/warehouse \
    --conf spark.hadoop.hive.metastore.warehouse.dir=/user/hive/warehouse \
    --conf spark.job.description="Read HDFS files, apply transformations, write to Hive external table" \
    -i "$JOB1_SCRIPT" 2>&1 | tee "$LOG_DIR/job1_$WORKFLOW_ID.log"

# Check if job failed
if [ $? -ne 0 ]; then
    echo "ERROR: Job 1 failed! See logs for details." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
    exit 1
fi

# Verify external table creation
echo "Verifying external table creation..." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
hdfs dfs -ls $HIVE_EXTERNAL_TABLE_PATH | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"

# Check if there's data in the output location
if hdfs dfs -count $HIVE_EXTERNAL_TABLE_PATH | awk '{print $2 + $3}' | grep -q "^0$"; then
    echo "ERROR: External table directory is empty!" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
    exit 1
else
    echo "External table data verified." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
fi

# Extract lineage information from job1 logs
echo "Extracting lineage information from Job 1..." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
grep "LINEAGE:" "$LOG_DIR/job1_$WORKFLOW_ID.log" > "$LOG_DIR/lineage_job1_$WORKFLOW_ID.log"

# Calculate time between jobs
JOB1_END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
echo "Job 1 completed at: $JOB1_END_TIME" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"

# Step 3: Run second Spark job: Asset Creation
echo "===================================================================="
echo "STEP 3: RUNNING JOB 2: Asset Creation with Lineage Tracking" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
echo "Start Time: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
echo "===================================================================="

$SPARK_HOME/bin/spark-shell \
    --master yarn \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf spark.workflow.id=$WORKFLOW_ID \
    --conf spark.sql.warehouse.dir=/user/hive/warehouse \
    --conf spark.hadoop.hive.metastore.warehouse.dir=/user/hive/warehouse \
    --conf spark.job.description="Read from external table, create analytics asset" \
    --conf spark.lineage.source=$HIVE_EXTERNAL_TABLE_PATH \
    -i "$JOB2_SCRIPT" 2>&1 | tee "$LOG_DIR/job2_$WORKFLOW_ID.log"

# Check if job failed
if [ $? -ne 0 ]; then
    echo "ERROR: Job 2 failed! See logs for details." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
    exit 1
fi

# Verify asset table creation
echo "Verifying asset table creation..." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
hdfs dfs -ls $HIVE_ASSET_TABLE_PATH | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"

# Check if there's data in the asset table location
if hdfs dfs -count $HIVE_ASSET_TABLE_PATH | awk '{print $2 + $3}' | grep -q "^0$"; then
    echo "ERROR: Asset table directory is empty!" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
    exit 1
else
    echo "Asset table data verified." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
fi

# Extract lineage information from job2 logs
echo "Extracting lineage information from Job 2..." | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
grep "LINEAGE:" "$LOG_DIR/job2_$WORKFLOW_ID.log" > "$LOG_DIR/lineage_job2_$WORKFLOW_ID.log"

# Record workflow completion
WORKFLOW_END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
echo "===================================================================="
echo "SPARK WORKFLOW COMPLETE" | tee -a "$LOG_DIR/workflow_$WORKFLOW_ID.log"
echo "End Time: $WORKFLOW_END_TIME" | tee -a "$LOG_DIR/workflow_$WORKFLOW_

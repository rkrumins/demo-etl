# Simple Spark Data Pipeline

A clean, straightforward Spark data pipeline that demonstrates creating and transforming Hive tables using join operations.

## Overview

This project includes:

1. **Data Generator** - Creates consistent CSV files with sample data
2. **Job 1** - Reads the source files, joins them, and creates a Hive table
3. **Job 2** - Reads from the Hive table, applies transformations, and creates a new analysis table
4. **Workflow Script** - Orchestrates the entire process

## Quick Start

To run the complete workflow:

```bash
# Make scripts executable
chmod +x generate-sample-data.sh run_workflow.sh

# Run the entire workflow
./run_workflow.sh
```

## Components

### Data Generator (`generate-sample-data.sh`)

Creates three CSV files with sample data:
- `customers.csv` - Customer information 
- `products.csv` - Product catalog
- `orders.csv` - Order transactions

These files are uploaded to HDFS at `/data/raw/`.

### Job 1 (`create_sales_table.scala`)

This Spark job:
- Reads the three source CSV files
- Performs LEFT JOIN operations to combine the data
- Creates a unified sales data table
- Writes the result to a Hive table named `sales_data`

### Job 2 (`transform_sales_data.scala`)

This Spark job:
- Reads from the `sales_data` Hive table
- Applies transformations to create additional columns:
  - Extended price, discount, and final price
  - Month name and day of week
  - Weekend flag
  - Product tier categorization
- Creates a new Hive table named `sales_analysis`
- Runs example analytics queries on the transformed data

### Workflow Script (`run_workflow.sh`)

Orchestrates the entire pipeline:
- Runs the data generator
- Executes Job 1 to create the initial Hive table
- Executes Job 2 to transform the data
- Verifies the created tables
- Logs all operations

## Data Flow

The data flows through the pipeline as follows:

```
CSV Files (customers, products, orders)
      │
      ▼
LEFT JOIN operations
      │
      ▼
   sales_data (Hive table)
      │
      ▼
Transformations (add columns)
      │
      ▼
sales_analysis (Hive table)
```

## Expected Outputs

1. Two Hive tables:
   - `sales_data` - The base sales information
   - `sales_analysis` - Enhanced data with additional calculations

2. Log files in the `./logs` directory:
   - `data-generation.log`
   - `create-sales-table.log`
   - `transform-sales-data.log`
   - `verify-tables.log`

## Requirements

- Apache Spark with Hive support
- Hadoop/HDFS
- Bash shell

## Next Steps

To extend this framework:
1. Add more complex transformations in Job 2
2. Implement data quality checks
3. Add partitioning to the Hive tables
4. Include data lineage tracking

## Troubleshooting

- Ensure HDFS is running and accessible
- Check that the Hive metastore is properly configured
- Verify that Spark has appropriate permissions to read/write to HDFS
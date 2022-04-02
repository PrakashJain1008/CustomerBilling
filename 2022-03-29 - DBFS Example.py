# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

  # File location and type
  file_location = "/FileStore/tables/Customer.csv"
  file_loc = "/FileStore/tables/BillData.csv"
  file_location_file = "/FileStore/tables/Location.csv"
  file_type = "csv"

  # CSV options
  infer_schema = "true"
  first_row_is_header = "true"
  delimiter = ","

  # The applied options are for CSV files. For other file types, these will be ignored.
  df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)

  df2 = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_loc)

  df3 = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location_file)

  display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "Customer_csv"
temp_bill_data = "bill_data"
temp_location_table = "location_table"

df.createOrReplaceTempView(temp_table_name)
df2.createOrReplaceTempView(temp_bill_data)
df3.createOrReplaceTempView(temp_location_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `Customer_csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `bill_data` order by cust_id asc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from location_table

# COMMAND ----------

# MAGIC %sql
# MAGIC with CTE as (
# MAGIC   select cust_id, sum(Amount) as Total_amount from bill_data group by cust_id
# MAGIC   )
# MAGIC   select c.name, ct.cust_id,l.Geoid,l.location, ct.Total_amount from CTE ct join Customer_csv c on ct.cust_id = c.cust_id join location_table l on l.Geoid = c.Geoid group by ct.cust_id,ct.Total_amount,c.name,l.location,l.Geoid order by ct.Total_amount desc limit 5

# COMMAND ----------

f# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "Customer_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql

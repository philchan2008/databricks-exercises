# Databricks notebook source
sales_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/upload/default/files/sales_data.csv")
store_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/upload/default/files/store_data.csv")
display(sales_df.sample(fraction=0.01))
display(store_df.sample(fraction=0.4))

# COMMAND ----------

from pyspark.sql.functions import col, avg

store_df = store_df.filter(col("store_id").isNotNull())
#display(store_df.select(avg("store_size")).first()[0])
avg_store_size = store_df.select(avg("store_size")).first()[0]
store_df = store_df.fillna({"store_size": avg_store_size})
store_df = store_df.filter(col("open_date").isNotNull())
display(store_df.sample(fraction=0.5))


# COMMAND ----------

from pyspark.sql.functions import col, to_date 

sales_df = sales_df.fillna({"quantity": 0, "total_amount": 0.0})
sales_df = sales_df.dropDuplicates()
sales_df = sales_df.filter(col("sale_id").isNotNull())
sales_df = sales_df.filter(col("store_id").isNotNull())
sales_df = sales_df.filter(col("sale_date").isNotNull())
sales_df = sales_df.filter((col("quantity") > 0) & (col("total_amount") > 0.0))

display(sales_df.sample(fraction=0.015))


# COMMAND ----------

sales_store_df = sales_df.join(store_df, on="store_id", how="inner")
display(sales_store_df.sample(fraction=0.01))

# COMMAND ----------

from pyspark.sql.functions import year, col, round

sales_store_df = sales_store_df.withColumn("sale_year", year(col("sale_date")))
sales_store_df = sales_store_df.filter(col("store_size").isNotNull())
sales_store_df = sales_store_df.withColumn("sales_per_sqft", round(col("total_amount") / col("store_size"), 2))
display(sales_store_df.sample(fraction=0.04))

# COMMAND ----------

sales_store_df.createOrReplaceTempView("sales_store")

# COMMAND ----------

sales_store_sql = spark.sql("""
    select 
        store_id, 
        store_region,
        round(sum(total_amount),0) as total_sales,
        sum(quantity) as total_quantity
    from sales_store
    group by store_id, store_region
""")
display(sales_store_sql)

# COMMAND ----------

top_product_sql = spark.sql("""
                                select 
                                    product_id,
                                    sum(quantity) as total_quantity
                                from sales_store
                                group by product_id
                                order by total_quantity desc
                                limit 5
                            """)
display(top_product_sql)

# COMMAND ----------

sales_store_sql.createOrReplaceTempView("store_sales_summary")
display(spark.sql("select * from store_sales_summary"))

# COMMAND ----------

top_store_sql = spark.sql("""
                        select store_id,
                        total_sales
                        from store_sales_summary
                        order by total_sales desc
                        limit 5
                    """)
display(top_store_sql)                    

# COMMAND ----------

top_product_sql.write.parquet("/Volumes/upload/default/files/top_product")
top_store_sql.write.parquet("/Volumes/upload/default/files/top_store")
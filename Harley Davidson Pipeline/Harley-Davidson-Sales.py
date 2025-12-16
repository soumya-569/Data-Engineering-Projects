# Databricks notebook source
# DBTITLE 1,Commence Session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('hd_Sales') \
        .config('spark.jars.packages','com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.37.0') \
            .master('yarn') \
                .getOrCreate()

# COMMAND ----------

# DBTITLE 1,Import Libraries
## Import Necessary Libraries

from pyspark.sql.functions import *
from pyspark.sql import Window
from datetime import datetime
import requests
import json

# COMMAND ----------
date_folder = datetime.now().date().isoformat()
# DBTITLE 1,Ingest Data
products = spark.read \
    .format('csv') \
        .option('header','true') \
            .option('mode','PERMISSIVE') \
                .load(f'gs://harley-davidson-data/Harley_Davidson/{date_folder}/Harley_Davidson_Product_Lookup.csv')

location = spark.read \
    .format('csv') \
        .option('header','true') \
            .option('mode','PERMISSIVE') \
                .load(f'gs://harley-davidson-data/Harley_Davidson/{date_folder}/Harley_Davidson_Territory_Lookup.csv')

product_category = spark.read \
    .format('csv') \
        .option('header','true') \
            .option('mode','PERMISSIVE') \
                .load(f'gs://harley-davidson-data/Harley_Davidson/{date_folder}/Harley_Davidson_Product_Categories_Lookup.csv')

product_subcategory = spark.read \
    .format('csv') \
        .option('header','true') \
            .option('mode','PERMISSIVE') \
                .load(f'gs://harley-davidson-data/Harley_Davidson/{date_folder}/Harley_Davidson_Product_Subcategories_Lookup.csv')

returns = spark.read \
    .format('csv') \
        .option('header','true') \
            .option('mode','PERMISSIVE') \
                .load(f'gs://harley-davidson-data/Harley_Davidson/{date_folder}/Harley_Davidson_Returns_Data.csv')

sales_2020 = spark.read \
    .format('csv') \
        .option('header','true') \
            .option('mode','PERMISSIVE') \
                .load(f'gs://harley-davidson-data/Harley_Davidson/{date_folder}/Harley_Davidson_Sales_Data_2020.csv')

sales_2021 = spark.read \
    .format('csv') \
        .option('header','true') \
            .option('mode','PERMISSIVE') \
                .load(f'gs://harley-davidson-data/Harley_Davidson/{date_folder}/Harley_Davidson_Sales_Data_2021.csv')

sales_2022 = spark.read \
    .format('csv') \
        .option('header','true') \
            .option('mode','PERMISSIVE') \
                .load(f'gs://harley-davidson-data/Harley_Davidson/{date_folder}/Harley_Davidson_Sales_Data_2022.csv')

customer = spark.read \
    .format('csv') \
        .option('header','true') \
            .option('mode','PERMISSIVE') \
                .load(f'gs://harley-davidson-data/Harley_Davidson/{date_folder}/Harley_Davidson_Customer_Lookup.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Change Data Types For Each Dataframe</h4>

# COMMAND ----------

# DBTITLE 1,Change Data Types
customer = customer.withColumns({
    'CustomerKey':col('CustomerKey').cast('int'),
    'Prefix':col('Prefix').cast('string'),
    'FirstName':col('FirstName').cast('string'),
    'LastName':col('LastName').cast('string'),
    'Gender':col('Gender').cast('string'),
    'BirthDate':col('BirthDate').cast('date'),
    'MaritalStatus':col('MaritalStatus').cast('string'),
    'EmailAddress':col('EmailAddress').cast('string'),
    'AnnualIncome':col('AnnualIncome').cast('double'),
    'TotalChildren':col('TotalChildren').cast('int'),
    'EducationLevel':col('EducationLevel').cast('string'),
    'Occupation':col('Occupation').cast('string'),
    'HomeOwner':col('HomeOwner').cast('string')
})

products = products.withColumns({
    'ProductKey':col('ProductKey').cast('int'),
    'ProductSubcategoryKey':col('ProductSubcategoryKey').cast('int'),
    'ProductSKU':col('ProductSKU').cast('string'),
    'ProductName':col('ProductName').cast('string'),
    'ModelName':col('ModelName').cast('string'),
    'ProductDescription':col('ProductDescription').cast('string'),
    'ProductColor':col('ProductColor').cast('string'),
    'ProductSize':col('ProductSize').cast('string'),
    'ProductStyle':col('ProductStyle').cast('string'),
    'ProductCost':col('ProductCost').cast('double'),
    'ProductPrice':col('ProductPrice').cast('double')
})

location = location.withColumns({
    'SalesTerritoryKey':col('SalesTerritoryKey').cast('int'),
    'Region':col('Region').cast('string'),
    'Country':col('Country').cast('string'),
    'Continent':col('Continent').cast('string')
})


product_category = product_category.withColumns({
    'ProductCategoryKey':col('ProductCategoryKey').cast('int'),
    'CategoryName':col('CategoryName').cast('string')
})

product_subcategory = product_subcategory.withColumns({
    'ProductSubcategoryKey':col('ProductSubcategoryKey').cast('int'),
    'SubcategoryName':col('SubcategoryName').cast('string'),
    'ProductCategoryKey':col('ProductCategoryKey').cast('int')
})

returns = returns.withColumns({
    'ReturnDate': col('ReturnDate').cast('date'),
    'TerritoryKey': col('TerritoryKey').cast('int'),
    'ProductKey': col('ProductKey').cast('int'),
    'ReturnQuantity': col('ReturnQuantity').cast('int')
})

sales_2020 = sales_2020.withColumns({
    'OrderDate':col('OrderDate').cast('date'),
    'StockDate':col('StockDate').cast('date'),
    'OrderNumber':col('OrderNumber').cast('string'),
    'ProductKey':col('ProductKey').cast('int'),
    'CustomerKey':col('CustomerKey').cast('int'),
    'TerritoryKey':col('TerritoryKey').cast('int'),
    'OrderLineItem':col('OrderLineItem').cast('int'),
    'OrderQuantity':col('OrderQuantity').cast('int')
})

sales_2021 = sales_2021.withColumns({
    'OrderDate':col('OrderDate').cast('date'),
    'StockDate':col('StockDate').cast('date'),
    'OrderNumber':col('OrderNumber').cast('string'),
    'ProductKey':col('ProductKey').cast('int'),
    'CustomerKey':col('CustomerKey').cast('int'),
    'TerritoryKey':col('TerritoryKey').cast('int'),
    'OrderLineItem':col('OrderLineItem').cast('int'),
    'OrderQuantity':col('OrderQuantity').cast('int')
})

sales_2022 = sales_2022.withColumns({
    'OrderDate':col('OrderDate').cast('date'),
    'StockDate':col('StockDate').cast('date'),
    'OrderNumber':col('OrderNumber').cast('string'),
    'ProductKey':col('ProductKey').cast('int'),
    'CustomerKey':col('CustomerKey').cast('int'),
    'TerritoryKey':col('TerritoryKey').cast('int'),
    'OrderLineItem':col('OrderLineItem').cast('int'),
    'OrderQuantity':col('OrderQuantity').cast('int')
})

# COMMAND ----------

# DBTITLE 1,Cleaning Dataframes
customer = customer.filter(col('CustomerKey').isNotNull())

# COMMAND ----------

# DBTITLE 1,Complete Sales 20-22
## Complete Sales Data 2020 - 2022

sales = sales_2020.union(sales_2021).union(sales_2022)

sales = sales.withColumns(
    {
        'order_month':month(col('OrderDate')),
        'order_year':year(col('OrderDate')),
        'order_week':dayofweek(col('OrderDate'))
    }
)

sales = sales.withColumn(
    'is_weekend',when(col('order_week').isin(1, 7), 'Yes').otherwise('No')
)

# COMMAND ----------

# DBTITLE 1,Complete Product
## Create complete product dataframe with product category and subcategory

comp_product = products.join(product_subcategory,on='ProductSubcategoryKey',how='inner').join(product_category,on='ProductCategoryKey',how='inner')

# COMMAND ----------

# DBTITLE 1,Financial Enrichment
## Financial Enrichment of Sales

product_sales = sales.join(products,on='ProductKey',how='inner')
product_sales = product_sales.withColumns(
    {
        'Revenue':col('OrderQuantity')*col('ProductPrice'),
        'Expense':col('OrderQuantity')*col('ProductCost')
    }
)

product_sales = product_sales.withColumn(
    'Profit',col('Revenue') - col('Expense')
)

# COMMAND ----------

# DBTITLE 1,Customer Enrichment
## Insert Full Name Column
customer = customer.withColumn('FullName',initcap(concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))).withColumn('Prefix',initcap(col('Prefix'))).withColumn('FirstName',initcap(col('FirstName'))).withColumn('LastName',initcap(col('LastName')))

## Compute Age in Years
customer = customer.withColumn('Age',round(datediff(current_date(),col('BirthDate'))/365.25,0))

## Insert Priority Columns As Per Customer's Income  (Threshold: <=50000 -> Low, >50000 & <=100000 -> Medium, >100000 -> High)
customer = customer.withColumn('Priority',when(col('AnnualIncome').between(0,50000),'Low').when(col('AnnualIncome').between(50000,100000),'Medium').otherwise('High'))

## Insert Is_Parent Column Based On Total Children Count
customer = customer.withColumn('IsParent',when(col('TotalChildren')>0,'Parent').otherwise('Not Parent'))


# COMMAND ----------

# DBTITLE 1,Load Into BigQuery
# Customer Dataframe
customer.write \
    .format('bigquery') \
        .option('table','budget-cloud-465616:Harley_Davidson_sales.customers') \
            .option('writeMethod','indirect') \
                .option('temporaryGcsBucket','spark-temp-bq') \
                    .mode('append') \
                        .save()

# Product Dataframe
comp_product.write \
    .format('bigquery') \
        .option('table','budget-cloud-465616:Harley_Davidson_sales.products_whole') \
            .option('writeMethod','indirect') \
                .option('temporaryGcsBucket','spark-temp-bq') \
                    .mode('append') \
                        .save()

# Location Dataframe
location.write \
    .format('bigquery') \
        .option('table','budget-cloud-465616:Harley_Davidson_sales.location') \
            .option('writeMethod','indirect') \
                .option('temporaryGcsBucket','spark-temp-bq') \
                    .mode('append') \
                        .save()

# Sales Dataframe
sales.write \
    .format('bigquery') \
        .option('table','budget-cloud-465616:Harley_Davidson_sales.sales') \
            .option('writeMethod','indirect') \
                .option('temporaryGcsBucket','spark-temp-bq') \
                    .mode('append') \
                        .save()

# Product Sales Dataframe
product_sales.write \
    .format('bigquery') \
        .option('table','budget-cloud-465616:Harley_Davidson_sales.product_sales') \
            .option('writeMethod','indirect') \
                .option('temporaryGcsBucket','spark-temp-bq') \
                    .mode('append') \
                        .save()

# Returns Dataframe
returns.write \
    .format('bigquery') \
        .option('table','budget-cloud-465616:Harley_Davidson_sales.returns') \
            .option('writeMethod','indirect') \
                .option('temporaryGcsBucket','spark-temp-bq') \
                    .mode('append') \
                        .save()

# Products Dataframe
products.write \
    .format('bigquery') \
        .option('table','budget-cloud-465616:Harley_Davidson_sales.products') \
            .option('writeMethod','indirect') \
                .option('temporaryGcsBucket','spark-temp-bq') \
                    .mode('append') \
                        .save()

# Product Category Dataframe
product_category.write \
    .format('bigquery') \
        .option('table','budget-cloud-465616:Harley_Davidson_sales.product_category') \
            .option('writeMethod','indirect') \
                .option('temporaryGcsBucket','spark-temp-bq') \
                    .mode('append') \
                        .save()

# Product Sub Category Dataframe
product_subcategory.write \
    .format('bigquery') \
        .option('table','budget-cloud-465616:Harley_Davidson_sales.product_subcategory') \
            .option('writeMethod','indirect') \
                .option('temporaryGcsBucket','spark-temp-bq') \
                    .mode('append') \
                        .save()
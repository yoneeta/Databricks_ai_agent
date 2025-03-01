# Databricks notebook source
# MAGIC %md
# MAGIC #  Welcome to the Databricks Notebook 
# MAGIC
# MAGIC  The Notebook is the primary code authoring tool in Databricks.  With it, you can do anything from simple exploratory data analysis to training ML models or building multi-stage data pipelines.
# MAGIC
# MAGIC Let's dive in and explore bakehouse data to analyze sales!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Lets get started 
# MAGIC
# MAGIC To run code in a Notebook cell, simply type your code and either click the Run cell button (top-left of the cell) or use `Cmd + Enter.`
# MAGIC </br>
# MAGIC </br>
# MAGIC <img src="https://docs.databricks.com/en/_images/cell-run-new.png">
# MAGIC
# MAGIC
# MAGIC Try running the provided statement: `print("Let's execute some Python code!")`.

# COMMAND ----------

print("Let's execute some Python code!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Try the command palette
# MAGIC
# MAGIC Use the `[Cmd + Shift + P]` keyboard shortcut to open the command palette for key notebook actions like inserting new cells, showing results side by side and more.
# MAGIC
# MAGIC Use it to insert a cell. 
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/command-palette.gif" width=500>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### The Notebook is a multi-language authoring experience
# MAGIC
# MAGIC You're not limited to just Python in Databricks! You can write and execute code in Python, SQL and you can annotate your notebooks using Markdown.
# MAGIC
# MAGIC ##### Let's go ahead and change the language using the top-right drop-down.
# MAGIC
# MAGIC - Insert a new cell below the current cell using the command palette. 
# MAGIC - Use the language switcher to change its language to SQL. Notice that `%sql` shows up at the top of the cell. This is called a [magic command](https://docs.databricks.com/en/notebooks/notebooks-code.html#mix-languages). 
# MAGIC - Type in `select "hello world";` and press run.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Access sample data
# MAGIC
# MAGIC In this exercise, we’ll use the Bakehouse dataset stored in samples.bakehouse, simulating bakery franchise data. Start by viewing the `samples.bakehouse.sales_transactions` table.
# MAGIC
# MAGIC On the left side of the Notebook, access the Catalog view to browse catalogs, schemas, and tables.
# MAGIC
# MAGIC To find the `sales_transactions` table, open the schema browser by clicking ![](https://docs.databricks.com/en/_images/notebook-data-icon.png) on the left, navigate to **Samples**, select the **bakehouse** schema and find the **sales_transactions** table, then click the kebab menu  <img src="https://docs.databricks.com/en/_images/kebab-menu.png"> next to the table and choose **Preview in new cell**.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 3: Explore and analyze data
# MAGIC Our bakehouse operates franchises across multiple countries, offering a variety of products.
# MAGIC
# MAGIC We’ll begin by identifying the most popular product by querying the sample data using Python.

# COMMAND ----------

# DBTITLE 1,Most popular product
import pandas as pd

# Read the sample bakehouse transactions and franchises datasets and load them into a PySpark DataFrame. 
df_transactions = spark.read.table('samples.bakehouse.sales_transactions')
df_franchises = spark.read.table('samples.bakehouse.sales_franchises')

# Convert PySpark DataFrame to Pandas DataFrame
pdf_transactions = df_transactions.toPandas()
pdf_franchises = df_franchises.toPandas()

# Which product sold the most units?
top_product = (pdf_transactions.groupby('product')['quantity']
          .sum()
          .reset_index()
          .sort_values(by='quantity', ascending=False)
         )
        
display(top_product)

# COMMAND ----------

# MAGIC %md
# MAGIC **Golden Gate Ginger** is our best-selling cookie! 
# MAGIC
# MAGIC To identify the top-performing city for its sales, we’ll join the `transactions` table with the `franchises` table. This will allow us to analyze which city sells the most units of Golden Gate Ginger.

# COMMAND ----------

# DBTITLE 1,Join franchises and transactions tables
## Top city selling most units of Golden Gate Ginger
top_city = (pdf_franchises.merge(pdf_transactions[pdf_transactions['product'] == 'Golden Gate Ginger'], 
                            on='franchiseID', 
                            how='right')
            .groupby('city')['quantity']
            .sum()
            .reset_index()
            .sort_values(by='quantity', ascending=False)
            .rename(columns={'quantity': 'units'})
         )

display(top_city)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Search and filter the Results table
# MAGIC
# MAGIC **Sort Results:**
# MAGIC Hover over a column name in the results table above, then click the arrow icon that appears to sort by that column’s values.
# MAGIC </br>
# MAGIC <img src="https://docs.databricks.com/en/_images/result-table-sort.png" width=350>
# MAGIC </br>
# MAGIC
# MAGIC - Try sorting the results table in ascending order above to find the city selling the least number of Golden Gate Ginger cookies. 
# MAGIC </br>
# MAGIC </br>
# MAGIC
# MAGIC **Filter results:**
# MAGIC To create a filter, click <img src="https://docs.databricks.com/en/_images/filter-icon.png"> at the upper-right of the cell results. In the dialog that appears, select the column to filter on and the filter rule and value to apply. 
# MAGIC - Try filtering for all cities selling **more than 100 units of Golden Gate Ginger** by typing in `units > 100`.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Visualize the data
# MAGIC
# MAGIC Let’s visualize **_weekly sales of Golden Gate Ginger across all locations_**.
# MAGIC
# MAGIC - Run the cell below to display the Golden Gate Ginger sales data. 
# MAGIC - To create a visualization, click the **+** button at the top of the results, then follow the steps in the visualization builder. 
# MAGIC - Choose your preferred chart type and set the chart values to complete your visualization.
# MAGIC
# MAGIC
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/new-visualization-menu.png" width=600>
# MAGIC
# MAGIC - View a sample visualization by clicking Golden Gate Ginger sales table in the Results section below.

# COMMAND ----------

# MAGIC %sql
# MAGIC --  How many units of Golden Gate Ginger are being sold across all locations every week?
# MAGIC SELECT
# MAGIC f.name as franchise_name, 
# MAGIC date_trunc('week',datetime) as week, 
# MAGIC sum(quantity) as quantity
# MAGIC FROM samples.bakehouse.sales_transactions t join samples.bakehouse.sales_franchises f on t.franchiseID = f.franchiseID
# MAGIC WHERE product = 'Golden Gate Ginger' 
# MAGIC GROUP BY 1,2
# MAGIC
# MAGIC -- Click Golden Gate Ginger Sales tab in the results section
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Use the Databricks Assistant for code suggestions
# MAGIC Notebooks come equipped with the context-aware [Databricks Assistant](https://docs.databricks.com/en/notebooks/use-databricks-assistant.html), which can help generate, explain, and fix code using natural language.
# MAGIC To use the assistant, create a new cell and click `CMD+I` or click <img src="https://docs.databricks.com/en/_images/help-assistant-icon.png"> on the top right corner of the new cell.
# MAGIC
# MAGIC Enter a prompt for the Assistant to provide code suggestions in the cell below. Here is a sample prompt:
# MAGIC - _Python code to show total units of Golden Gate Ginger sold._
# MAGIC
# MAGIC Click **Generate** or press **Enter** on the prompt and watch the Assistant suggest code to answer the prompt. Click “Accept” to save the code suggestion and run the cell to view the results!
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### AI Assistant can also help fix errors
# MAGIC Run the query below. When an error occurs, **[Quick Fix](https://docs.databricks.com/en/notebooks/use-databricks-assistant.html)** will automatically suggest solutions for basic issues that can be resolved with a single-line change.
# MAGIC Click **Accept and run** to apply the fix and continue running your code.
# MAGIC
# MAGIC </br>
# MAGIC <img src="https://docs.databricks.com/en/_images/assistant-quick-fix.png" width=500>

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_trunc('WEEK', dateTime), sum(quantity) as totals
# MAGIC from samples.bakehouse.sales_transactions
# MAGIC where product = 'Golden Gate Ginger'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Continue exploring Notebooks!
# MAGIC
# MAGIC - To learn about adding data from CSV files to Unity Catalog and visualize data, see [Get started: Import and visualize CSV data from a notebook](https://docs.databricks.com/en/getting-started/import-visualize-data.html).
# MAGIC - To learn how to load data into Databricks using Apache Spark, see [Tutorial: Load and transform data using Apache Spark DataFrames](https://docs.databricks.com/en/getting-started/dataframes.html).
# MAGIC - To learn more about visualizations, see [Visualizations in Databricks notebooks](https://raw.githubusercontent.com/databricks/tmm/refs/heads/main/eda-starter-notebook/Visualizations in Databricks notebooks).
# MAGIC
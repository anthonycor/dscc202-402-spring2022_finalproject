# Databricks notebook source
# MAGIC %md
# MAGIC ## Token Recommendation
# MAGIC <table border=0>
# MAGIC   <tr><td><img src='https://data-science-at-scale.s3.amazonaws.com/images/rec-application.png'></td>
# MAGIC     <td>Your application should allow a specific wallet address to be entered via a widget in your application notebook.  Each time a new wallet address is entered, a new recommendation of the top tokens for consideration should be made. <br> **Bonus** (3 points): include links to the Token contract on the blockchain or etherscan.io for further investigation.</td></tr>
# MAGIC   </table>

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address, start_date = Utils.create_widgets()
print(wallet_address, start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your code starts here...

# COMMAND ----------

sql_statement = "SELECT * FROM G01_db.SilverTable_externalwallets WHERE WalletHash='" + str(wallet_address) + "';"
df = spark.sql(sql_statement)

# COMMAND ----------

display(df.toPandas().to_html())

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

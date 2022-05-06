# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Implement a routine to "promote" your model at **Staging** in the registry to **Production** based on a boolean flag that you set in the code.
# MAGIC - Using wallet addresses from your **Staging** and **Production** model test data, compare the recommendations of the two models.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address,start_date = Utils.create_widgets()
print(wallet_address,start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your Code Starts Here...

# COMMAND ----------

from delta.tables import *
from pyspark.ml.evaluation import RegressionEvaluator

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature
from mlflow.models.signature import ModelSignature


sqlContext.setConf('spark.sql.shuffle.partitions', 'auto')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Staging Model

# COMMAND ----------

def evaluate_push_staging_production(staging_name, production_name, switch=False):
    evalutator = RegressionEvaluator(predictionCol='prediction', labelCol='Balance', metricName='rmse')
    testing_data = DeltaTable.forPath(spark, '/user/hive/warehouse/g01_db.db/silvertable_walletbalance/').history().head(1)[0]['version'].sample(0.3)
    
    staging_model = mlflow.spark.load_model('models:/' + staging_name + '/Staging')
    staging_model_predictions = model.transform(self.test_df)  # View the predictions
    staging_RMSE = evalutator.evaluate(staging_model_predictions)
    print(f'Staging Model Root-mean-square error on the test dataset = {staging_RMSE}')
    
    predict_model = mlflow.spark.load_model('models:/' + staging_name + '/Production')
    predict_model_predictions = model.transform(self.test_df)  # View the predictions
    predict_RMSE = evalutator.evaluate(predict_model_predictions)
    print(f'Production Model Root-mean-square error on the test dataset = {predict_RMSE}')
    
    if staging_RMSE < predict_RMSE and switch:
        print(f'Relegating {production_name} to archive, pushing {staging_name} to production.')
        
        # Capture the latest model version, archive any previous Staged version, Transition this version to Staging
        client = MlflowClient()
        
        client.transition_model_version_stage(
            name=staging_name,
            version=model_versions[0],
            stage='production')

# COMMAND ----------

evaluate_push_staging_production(staging_name='FirstAttempt', production_name='SomethingElse', switch=True)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

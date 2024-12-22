from pyspark.sql import SparkSession
from cryptography.fernet import Fernet
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
import os

spark = SparkSession.builder \
    .appName("spark_insert_postgres") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://localhost:5432/final_project"
jdbc_properties = {
    "user": "postgres",
    "password": "choichoichoi954",
    "driver": "org.postgresql.Driver"
}

df_cashier_data = spark.read.csv("/home/hadoop/final_project/csv_inputs_and_dummies/cashier_data.csv", header=True)
df_customer_data = spark.read.csv("/home/hadoop/final_project/csv_inputs_and_dummies/customer_data.csv", header=True)

cashier_data_key = Fernet.generate_key()
cashier_cipher =Fernet(cashier_data_key)

customer_data_key = Fernet.generate_key()
customer_cipher =Fernet(customer_data_key)

def encrypt_data_cashier(data):
    """Encrypt the data using Fernet"""
    return cashier_cipher.encrypt(data.encode()).decode()

def encrypt_data_customer(data):
    """Encrypt the data using Fernet"""
    return customer_cipher.encrypt(data.encode()).decode()

encrypt_udf_cashier = udf(encrypt_data_cashier, StringType())
encrypt_udf_customer = udf(encrypt_data_customer, StringType())

df_cashier_data = df_cashier_data.withColumn("cashier_email", encrypt_udf_cashier(df_cashier_data["cashier_email"]))
df_cashier_data = df_cashier_data.withColumn("cashier_phone_number", encrypt_udf_cashier(df_cashier_data["cashier_phone_number"]))

df_customer_data = df_customer_data.withColumn("customer_email", encrypt_udf_customer(df_customer_data["customer_email"]))
df_customer_data = df_customer_data.withColumn("customer_phone_number", encrypt_udf_customer(df_customer_data["customer_phone_number"]))

df_cashier_data = df_cashier_data.drop("_c0")
df_cashier_data = df_cashier_data.withColumn("cashier_id", df_cashier_data["cashier_id"].cast(IntegerType()))
df_cashier_data.write.jdbc(url=jdbc_url, table="cashier_data", mode="overwrite", properties=jdbc_properties)

df_customer_data = df_customer_data.drop("_c0")
df_customer_data = df_customer_data.withColumn("customer_id", df_customer_data["customer_id"].cast(IntegerType()))
df_customer_data.write.jdbc(url=jdbc_url, table="customer_data", mode="overwrite", properties=jdbc_properties)

cashier_key_file_path = "/home/hadoop/final_project/csv_output/keys/cashier_key.key"  # Change to your secure directory
customer_key_file_path = "/home/hadoop/final_project/csv_output/keys/customer_key.key"

os.makedirs(os.path.dirname(cashier_key_file_path), exist_ok=True)
os.makedirs(os.path.dirname(customer_key_file_path), exist_ok=True)

# Load the key from the file
with open(cashier_key_file_path, "wb") as key_file:
    key = key_file.write(cashier_data_key)

with open(customer_key_file_path, "wb") as key_file:
    key = key_file.write(customer_data_key)
# Notebook initialize boilerplate
from dotenv import load_dotenv; load_dotenv()
from data_source import catalog
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()
cat = catalog.load()
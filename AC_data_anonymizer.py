#Part 3. SPARK APPLICATION  ###############################################
#######  Download file from s3 and SHA256 encode with native spark function

from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

spark = SparkSession.builder.appName('data_hash').getOrCreate()

s3_bucket = '<bucket_name>'
file_name = 'personal_info.csv'
download_path = '/anonymization/'+file_name'
df_columns = ['name', 'street_address', 'state', 'postcode', 'birthdate']

df = spark.read.csv('s3://'+s3_bucket+download_path, header = False)
df = toDF(*df_columns)
hashed_df = df.select(
                        sha2('name',256),
                        sha2('street_address',256),
                        'state',
                        'post_code',
                        sha2('birthdate', 256)
).show()

spark.stop()
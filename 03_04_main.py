from pyspark.sql import SparkSession
from pyspark.sql.functions import col

S3_SOURCE_DATA_LOC = 's3://deprocessingdemo/source_data/survey_results_public.csv'
S3_OUTPUT_DATA_LOC = 's3://deprocessingdemo/output_data/'

def main():
  spark = SparkSession.builder.appName('MyApp').getOrCreate()
  all_data = spark.read.csv(S3_SOURCE_DATA_LOC, header=True)
  print('Total number of records in the source data: %s' % all_data.count())
  selected_data = all_data.where((col('Country') == 'India') & (col('WorkWeekHrs') > 45))
  print('The number of Workaohlic engineers in India is: %s' % selected_data.count())
  selected_data.write.mode('overwrite').parquet(S3_OUTPUT_DATA_LOC)
  
if __name__ == '__main__':
  main()
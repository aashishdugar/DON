sqlContext = SQLContext(sc)
df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', delimiter='\t', inferschema='true').load('/user/ab7325/data.gdeltproject.org/events/*.CSV')
from pyspark.sql.functions import desc
cs = df.groupBy('_c7').count()
cs.sort(desc('count')).show(200)

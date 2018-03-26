from pyspark.sql.functions import desc
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', delimiter='\t', inferschema='true').load('/user/ab7325/data.gdeltproject.org/events/*.CSV')
cs = df.groupBy('_c7').count()
cs.sort(desc('count')).show(200)

dis = sqlContext.read.format('com.databricks.spark.csv').options(header='true', delimiter='\t', inferschema='true').load('/user/ab7325/data.gdeltproject.org/gkg/*.csv')
ds = dis.filter(dis['COUNTTYPE']=='SICKENED').groupBy('GEO_COUNTRYCODE').count()
ds.sort(desc('count')).show(200)

events = sqlContext.read.format('com.databricks.spark.csv').options(header='true', delimiter=',', inferschema='true').load('/user/ab7325/disease/disease_events.csv')

from pyspark.sql.functions import desc
import datetime
from pyspark.sql.functions import udf, to_date, format_string, col
from pyspark.sql.types import DateType
import pandas as pd

sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='false', delimiter='\t', inferschema='true').load('/user/ab7325/data.gdeltproject.org/events/*.CSV')
cs = df.groupBy('_c7').count()
cs.sort(desc('count')).show(200)
dis = sqlContext.read.format('com.databricks.spark.csv').options(header='true', delimiter='\t', inferschema='true').load('/user/ab7325/data.gdeltproject.org/gkg/*.csv')
#ds = dis.filter(dis['COUNTTYPE']=='SICKENED').groupBy('GEO_COUNTRYCODE').count()
#ds.sort(desc('count')).show(200)


events = sqlContext.read.format('com.databricks.spark.csv').options(header='true', delimiter=',', inferschema='true').load('/user/ab7325/disease/disease_events.csv')
filtered = events.select(to_date(events.Timestamp, 'dd-mm-yyyy').alias('filtered_date'), "Country", "Disease")
filtered = filtered.groupBy(['filtered_date', 'Country']).count()
filtered = filtered.select(col('count').alias('un_count'), 'filtered_date', 'Country')
dis = sqlContext.read.format('com.databricks.spark.csv').options(header='true', delimiter='\t', inferschema='true').load('/user/ab7325/data.gdeltproject.org/gkg/*.csv')
ds = dis.filter(dis['COUNTTYPE']=='SICKENED').select(to_date(format_string('%d', dis.DATE), 'yyyymmdd').alias('datetime'), '*')
ds = ds.groupBy(['datetime', 'GEO_COUNTRYCODE']).count()
ds = ds.select(col('count').alias('gdelt_count'), 'datetime', 'GEO_COUNTRYCODE')
countries = sqlContext.read.format('com.databricks.spark.csv').options(header='true', delimiter=',', inferschema='true').load('/user/ab7325/disease/gdelt_geo_country_codes.csv')
countries = countries.select(col('FIPS 10-4').alias('geocode'), 'Name')
ds = ds.join(countries, ds.GEO_COUNTRYCODE == countries.geocode, 'inner')
count_joined = ds.join(filtered, (filtered.Country == ds.Name) & (filtered.filtered_date == ds.datetime), 'outer')
count_df = count_joined.toPandas()
count_df.to_pickle('/home/ab7325/joined_events')

count_joined.filter(col('datetime').isNotNull() & col('filtered_date').isNotNull()).show(200)

joined = ds.join(filtered, filtered.filtered_date==ds.datetime, 'outer')
joined = joined.select(col('filtered_date').isNull().cast('integer').alias('bool_ground_date'), col('datetime').isNull().cast('integer').alias('bool_gdelt_date'), '*')
joined.stat.corr('bool_ground_date', 'bool_gdelt_date')
rslt = joined.collect()

inner_join =  ds.join(filtered, filtered.date==ds.datetime, 'inner')
inner_rslt = inner_join.collect()

#Saudi Arabia

ds = dis.filter((dis['GEO_COUNTRYCODE']=='SA') | (dis['GEO_COUNTRYCODE']=='BA') | (dis['GEO_COUNTRYCODE']=='EG') | (dis['GEO_COUNTRYCODE']=='IR') | (dis['GEO_COUNTRYCODE']=='JO') | (dis['GEO_COUNTRYCODE']=='KU') | (dis['GEO_COUNTRYCODE']=='LE') | (dis['GEO_COUNTRYCODE']=='MU') | (dis['GEO_COUNTRYCODE']=='QA') | (dis['GEO_COUNTRYCODE']=='TS') | (dis['GEO_COUNTRYCODE']=='AE') | (dis['GEO_COUNTRYCODE']=='YM'))
ds = ds.filter((dis['COUNTTYPE']=='KILL') | (dis['COUNTTYPE']=='WOUND') | (dis['COUNTTYPE']=='SICKENED') | (dis['COUNTTYPE']=='AFFECT'))
rslt = ds.select(col('DATE'), col('NUMBER'), col('GEO_COUNTRYCODE'), col('COUNTTYPE'))
rslt.toPandas().to_pickle('/home/ab7325/me_events')

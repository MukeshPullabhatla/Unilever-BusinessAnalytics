spark.sql("drop database Unilever_Spark_BAODB CASCADE")

spark.sql("create database Unilever_Spark_BAODB")

val prop = new java.util.Properties
prop.setProperty("driver","com.mysql.jdbc.Driver")
prop.setProperty("user","root")
prop.setProperty("password","root")

spark.sql("CREATE TABLE IF NOT EXISTS Unilever_Spark_BAODB.dove(prod_name string,type_of_prod string,prod_id int ,qty int,date_of_mfg string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'  ")
spark.sql("load data inpath '/Unilever_SparkProcessing/dove' into table Unilever_Spark_BAODB.dove")
val res=spark.sql("select * from Unilever_Spark_BAODB.dove")

res.write.mode("append").jdbc("jdbc:mysql://localhost:3306/batch97", "dovetab", prop)

res.show
res.write.partitionBy("type_of_prod").bucketBy(4,"prod_id").format("csv").saveAsTable("Unilever_Spark_BAODB.partbuckettabdove")
spark.sql("select * from Unilever_Spark_BAODB.partbuckettabdove TABLESAMPLE(bucket 2 out of 4) where type_of_prod = 'BESTSELLER'").show
spark.sql("create view  Unilever_Spark_BAODB.doveview1 as select prod_name ,prod_id ,qty , from_unixtime(unix_timestamp(date_of_mfg,'dd-mm-yyyy'),'yyyy-mm-dd' ) as newmdate,to_date(from_unixtime(unix_timestamp())) as cdate ,floor(datediff(to_date(from_unixtime(unix_timestamp())),from_unixtime(unix_timestamp(date_of_mfg,'dd-mm-yyyy'),'yyyy-mm-dd' ))/365) as YearOfExp from Unilever_Spark_BAODB.partbuckettabdove")
spark.sql("select * from Unilever_Spark_BAODB.doveview1").show

spark.sql("CREATE TABLE IF NOT EXISTS Unilever_Spark_BAODB.lipton(prod_name string,type_of_prod string,prod_id int ,qty int,date_of_mfg string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'  ")
spark.sql("load data inpath '/Unilever_SparkProcessing/lipton' into table Unilever_Spark_BAODB.lipton")
val res=spark.sql("select * from Unilever_Spark_BAODB.lipton")

res.write.mode("append").jdbc("jdbc:mysql://localhost:3306/batch97", "liptontab", prop)
res.show

res.write.partitionBy("type_of_prod").bucketBy(4,"prod_id").format("csv").saveAsTable("Unilever_Spark_BAODB.partbuckettablipton")
spark.sql("select * from Unilever_Spark_BAODB.partbuckettablipton TABLESAMPLE(bucket 2 out of 4) where type_of_prod = 'BESTSELLER'").show
spark.sql("create view  Unilever_Spark_BAODB.liptonview1 as select prod_name ,prod_id ,qty , from_unixtime(unix_timestamp(date_of_mfg,'dd-mm-yyyy'),'yyyy-mm-dd' ) as newmdate,to_date(from_unixtime(unix_timestamp())) as cdate ,floor(datediff(to_date(from_unixtime(unix_timestamp())),from_unixtime(unix_timestamp(date_of_mfg,'dd-mm-yyyy'),'yyyy-mm-dd' ))/365) as YearOfExp from Unilever_Spark_BAODB.partbuckettablipton")
spark.sql("select * from Unilever_Spark_BAODB.liptonview1").show

spark.sql("CREATE TABLE IF NOT EXISTS Unilever_Spark_BAODB.domestos(prod_name string,type_of_prod string,prod_id int ,qty int,date_of_mfg string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'  ")
spark.sql("load data inpath '/Unilever_SparkProcessing/domestos' into table Unilever_Spark_BAODB.domestos")
val res=spark.sql("select * from Unilever_Spark_BAODB.domestos")

res.write.mode("append").jdbc("jdbc:mysql://localhost:3306/batch97", "domestostab", prop)
res.show
res.write.partitionBy("type_of_prod").bucketBy(4,"prod_id").format("csv").saveAsTable("Unilever_Spark_BAODB.partbuckettabdomestos")
spark.sql("select * from Unilever_Spark_BAODB.partbuckettabdomestos TABLESAMPLE(bucket 2 out of 4) where type_of_prod = 'BESTSELLER'").show
spark.sql("create view  Unilever_Spark_BAODB.domestosview1 as select prod_name ,prod_id ,qty, from_unixtime(unix_timestamp(date_of_mfg,'dd-mm-yyyy'),'yyyy-mm-dd' ) as newmdate,to_date(from_unixtime(unix_timestamp())) as cdate ,floor(datediff(to_date(from_unixtime(unix_timestamp())),from_unixtime(unix_timestamp(date_of_mfg,'dd-mm-yyyy'),'yyyy-mm-dd' ))/365) as YearOfExp from Unilever_Spark_BAODB.partbuckettabdomestos")
spark.sql("select * from Unilever_Spark_BAODB.domestosview1").show

spark.sql("CREATE TABLE IF NOT EXISTS Unilever_Spark_BAODB.other(prod_name string,type_of_prod string,prod_id int ,qty int,date_of_mfg string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'  ")
spark.sql("load data inpath '/Unilever_SparkProcessing/other' into table Unilever_Spark_BAODB.other")
val res=spark.sql("select * from Unilever_Spark_BAODB.other")

res.write.mode("append").jdbc("jdbc:mysql://localhost:3306/batch97", "otherstab", prop)
res.show
res.write.partitionBy("type_of_prod").bucketBy(4,"prod_id").format("csv").saveAsTable("Unilever_Spark_BAODB.partbuckettabother")
spark.sql("select * from Unilever_Spark_BAODB.partbuckettabother TABLESAMPLE(bucket 2 out of 4) where type_of_prod = 'BESTSELLER'").show
spark.sql("create view  Unilever_Spark_BAODB.otherview1 as select prod_name ,prod_id ,qty , from_unixtime(unix_timestamp(date_of_mfg,'dd-mm-yyyy'),'yyyy-mm-dd' ) as newmdate,to_date(from_unixtime(unix_timestamp())) as cdate ,floor(datediff(to_date(from_unixtime(unix_timestamp())),from_unixtime(unix_timestamp(date_of_mfg,'dd-mm-yyyy'),'yyyy-mm-dd' ))/365) as YearOfExp from Unilever_Spark_BAODB.partbuckettabother")
spark.sql("select * from Unilever_Spark_BAODB.otherview1").show
System.exit(0)

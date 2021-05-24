val rdd1= sc.textFile("hdfs://localhost:8020/unilever_output_bao_path/part-r-00000")
val rdd2=rdd1.map(y=>y.replace('\t',',')).distinct
val rdd3=rdd2.map(x=>x.split(",")).sortBy(x=>x(2))
val rdd4=rdd3.map { x =>
      val prod_name=x(0)
      val type_of_prod=x(1)
      val prod_id=x(2).toInt
      val qty=x(3).toInt
      val date_of_mfg=x(4)
      prod_name+','+type_of_prod+','+prod_id+','+qty+','+date_of_mfg
      }
rdd4.coalesce(1).saveAsTextFile("hdfs://localhost:8020/Unilever_SparkProcessing/dove")

val rdd1= sc.textFile("hdfs://localhost:8020/unilever_output_bao_path/part-r-00001")
val rdd2=rdd1.map(y=>y.replace('\t',',')).distinct
val rdd3=rdd2.map(x=>x.split(",")).sortBy(x=>x(2))
val rdd4=rdd3.map { x =>
      val prod_name=x(0)
      val type_of_prod=x(1)
      val prod_id=x(2).toInt
      val qty=x(3).toInt
      val date_of_mfg=x(4)
      prod_name+','+type_of_prod+','+prod_id+','+qty+','+date_of_mfg
      }
rdd4.coalesce(1).saveAsTextFile("hdfs://localhost:8020/Unilever_SparkProcessing/lipton")

val rdd1= sc.textFile("hdfs://localhost:8020/unilever_output_bao_path/part-r-00002")
val rdd2=rdd1.map(y=>y.replace('\t',',')).distinct
val rdd3=rdd2.map(x=>x.split(",")).sortBy(x=>x(2))
val rdd4=rdd3.map { x =>
      val prod_name=x(0)
      val type_of_prod=x(1)
      val prod_id=x(2).toInt
      val qty=x(3).toInt
      val date_of_mfg=x(4)
      prod_name+','+type_of_prod+','+prod_id+','+qty+','+date_of_mfg
      }
rdd4.coalesce(1).saveAsTextFile("hdfs://localhost:8020/Unilever_SparkProcessing/domestos")

val rdd1= sc.textFile("hdfs://localhost:8020/unilever_output_bao_path/part-r-00003")
val rdd2=rdd1.map(y=>y.replace('\t',',')).distinct
val rdd3=rdd2.map(x=>x.split(",")).sortBy(x=>x(2))
val rdd4=rdd3.map { x =>
      val prod_name=x(0)
      val type_of_prod=x(1)
      val prod_id=x(2).toInt
      val qty=x(3).toInt
      val date_of_mfg=x(4)
      prod_name+','+type_of_prod+','+prod_id+','+qty+','+date_of_mfg
      }
rdd4.coalesce(1).saveAsTextFile("hdfs://localhost:8020/Unilever_SparkProcessing/other")
System.exit(0)

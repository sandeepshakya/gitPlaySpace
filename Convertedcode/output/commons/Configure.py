def configureSession(sqlContext):
	sqlContext.sql("set hive.exec.drop.ignorenonexistent=true")
	sqlContext.sql("set spark.sql.hive.convertMetastoreParquet=false")
	sqlContext.sql("set spark.sql.crossJoin.enabled = true")
	sqlContext.sql("set spark.sql.shuffle.partitions=2")

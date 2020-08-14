from datetime import datetime
from pyspark.sql.types import *
import logging
import sys
import os
def executeMapping(workflowObject, args):
	global m_SS_CCO_SALES_FACT_Demo_properties
	global wf
	global performance
	wf = workflowObject
	m_SS_CCO_SALES_FACT_Demo_properties = args['property']
	performance = args['performanceObject']
	HIVE_PREPARATION_SCHEMA_NAME = m_SS_CCO_SALES_FACT_Demo_properties.HIVE_PREPARATION_SCHEMA_NAME
	################################################################################################################################
	#	Informatica Enity Name : SQ_SALES
	#	Informatica Type : SOURCE QUALIFIER
	#	Mapping Name : m_SS_CCO_SALES_FACT_Demo
	################################################################################################################################
	INFORMATICA_DATABASE = m_SS_CCO_SALES_FACT_Demo_properties.src_database_SQ_SALES;
	performance.executeSql("use " + INFORMATICA_DATABASE);
	df0 = wf.executeWorkflowSql("""SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) as UNIQUE_ID,
	SALES.CCO_SFID AS CCO_SFID,
	SALES.INVOICE_ID AS INVOICE_ID,
	SALES.PRODUCT_ID AS PRODUCT_ID,
	SALES.SALES_DT AS SALES_DT,
	SALES.UNIT_SOLD AS UNIT_SOLD,
	SALES.UNIT_PRICE AS UNIT_PRICE,
	SALES.TOT_AMT AS TOT_AMT FROM """ + m_SS_CCO_SALES_FACT_Demo_properties.SQ_SALES_DB_NAME + """.sales""", 'SQ_SALES_1', dbType=performance.resolveDBTypeForJDBCConnection(m_SS_CCO_SALES_FACT_Demo_properties.SQ_SALES_JDBC_DB_TYPE), databaseName=m_SS_CCO_SALES_FACT_Demo_properties.SQ_SALES_DB_NAME, targetTableName=None, isLoad=True);
	performance.performanceTuning(df0, """{'databaseName' : '""" + HIVE_PREPARATION_SCHEMA_NAME + """', 'typeOfInsert' : 'OVERRIDE', 'tableName' : 'qualifier_SQ_SALES_0', 'queryId' : 'WM_0'}""", False);
	################################################################################################################################
	#	Informatica Enity Name : EXP_SALES
	#	Informatica Type : EXPRESSION
	#	Mapping Name : m_SS_CCO_SALES_FACT_Demo
	#	Comments and Information: Please go through the following points:
	# 	            1. The Number of Inputs For expression are : 1
	# 	            2. Number of I/O Present are : 7 
	# 	            3. Number of Running local Variable Present are : 0
	################################################################################################################################
	df1 = wf.executeWorkflowSql("""SELECT
	        in0.UNIQUE_ID,
	        LTRIM_UDF (  RTRIM_UDF (  in0.CCO_SFID  )    ) AS o_CCO_SFID,
	        in0.INVOICE_ID AS INVOICE_ID,
	        in0.PRODUCT_ID AS PRODUCT_ID,
	        in0.SALES_DT AS SALES_DT,
	        in0.UNIT_SOLD AS UNIT_SOLD,
	        in0.UNIT_PRICE AS UNIT_PRICE,
	        in0.TOT_AMT AS TOT_AMT  
	    FROM
	        qualifier_SQ_SALES_0 AS in0""", 'Exp_SALES_1');
	performance.performanceTuning(df1, """{'databaseName' : '""" + HIVE_PREPARATION_SCHEMA_NAME + """', 'typeOfInsert' : 'OVERRIDE', 'tableName' : 'Exp_SALES_out_1_0', 'queryId' : 'WM_1'}""", False);
	################################################################################################################################
	#	Informatica Enity Name : SQ_CCO
	#	Informatica Type : SOURCE QUALIFIER
	#	Mapping Name : m_SS_CCO_SALES_FACT_Demo
	################################################################################################################################
	INFORMATICA_DATABASE = m_SS_CCO_SALES_FACT_Demo_properties.src_database_SQ_CCO;
	performance.executeSql("use " + INFORMATICA_DATABASE);
	df2 = wf.executeWorkflowSql("""SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) as UNIQUE_ID,
	CCO.CCO_ID AS CCO_ID,
	CCO.CCO_NAME AS CCO_NAME,
	CCO.CCO_SFID AS CCO_SFID,
	CCO.ACTV_FL AS ACTV_FL,
	CCO.START_DT AS START_DT FROM """ + m_SS_CCO_SALES_FACT_Demo_properties.SQ_CCO_DB_NAME + """.cco""", 'SQ_CCO_1', dbType=performance.resolveDBTypeForJDBCConnection(m_SS_CCO_SALES_FACT_Demo_properties.SQ_CCO_JDBC_DB_TYPE), databaseName=m_SS_CCO_SALES_FACT_Demo_properties.SQ_CCO_DB_NAME, targetTableName=None, isLoad=True);
	performance.performanceTuning(df2, """{'databaseName' : '""" + HIVE_PREPARATION_SCHEMA_NAME + """', 'typeOfInsert' : 'OVERRIDE', 'tableName' : 'qualifier_SQ_CCO_0', 'queryId' : 'WM_2'}""", False);
	################################################################################################################################
	#	Informatica Enity Name : EXP_CCO
	#	Informatica Type : EXPRESSION
	#	Mapping Name : m_SS_CCO_SALES_FACT_Demo
	#	Comments and Information: Please go through the following points:
	# 	            1. The Number of Inputs For expression are : 1
	# 	            2. Number of I/O Present are : 3 
	# 	            3. Number of Running local Variable Present are : 0
	################################################################################################################################
	df3 = wf.executeWorkflowSql("""SELECT
	        in0.UNIQUE_ID,
	        in0.CCO_ID AS CCO_ID,
	        in0.CCO_NAME AS CCO_NAME,
	        LTRIM_UDF (  RTRIM_UDF (  in0.CCO_SFID  )    ) AS o_CCO_SFID  
	    FROM
	        qualifier_SQ_CCO_0 AS in0""", 'Exp_CCO_1');
	performance.performanceTuning(df3, """{'databaseName' : '""" + HIVE_PREPARATION_SCHEMA_NAME + """', 'typeOfInsert' : 'OVERRIDE', 'tableName' : 'Exp_CCO_out_1_0', 'queryId' : 'WM_3'}""", False);
	################################################################################################################################
	#	Informatica Enity Name : JNR_CCO_SALES
	#	Informatica Type : JOINER
	#	Mapping Name : m_SS_CCO_SALES_FACT_Demo
	################################################################################################################################
	df4 = wf.executeWorkflowSql("""  SELECT 
	ROW_NUMBER() OVER(ORDER BY m.UNIQUE_ID) as UNIQUE_ID, 
	m.CCO_ID AS CCO_ID,
	m.CCO_NAME AS CCO_NAME,
	m.o_CCO_SFID AS o_CCO_SFID,
	d.o_CCO_SFID AS o_CCO_SFID1,
	d.INVOICE_ID AS INVOICE_ID,
	d.PRODUCT_ID AS PRODUCT_ID,
	d.SALES_DT AS SALES_DT,
	d.UNIT_SOLD AS UNIT_SOLD,
	d.UNIT_PRICE AS UNIT_PRICE,
	d.TOT_AMT AS TOT_AMT 
	FROM 
	Exp_CCO_out_1_0 AS m  
	 INNER JOIN  
	Exp_SALES_out_1_0 AS d  
	ON 
	  m.o_CCO_SFID  =  d.o_CCO_SFID  """, 'Jnr_CCO_Sales_1');
	performance.performanceTuning(df4, """{'databaseName' : '""" + HIVE_PREPARATION_SCHEMA_NAME + """', 'typeOfInsert' : 'OVERRIDE', 'tableName' : 'Jnr_CCO_Sales_out_1_0', 'queryId' : 'WM_4'}""", False);
	################################################################################################################################
	#	Informatica Enity Name : LKP_DATE_DIM
	#	Informatica Type : CONNECTED LOOKUP
	#	Mapping Name : m_SS_CCO_SALES_FACT_Demo
	################################################################################################################################
	#performance.executeSql("""CREATE TABLE """ + INFORMATICA_DATABASE + """.DATE_DIM ( UNIQUE_ID bigint, DT_KY double,DT_ID timestamp,SALES_DT timestamp) STORED AS ORC""");
	INFORMATICA_DATABASE = m_SS_CCO_SALES_FACT_Demo_properties.lkp_database_LKP_DATE_DIM;
	performance.executeSql("use " + INFORMATICA_DATABASE);
	df_jdbc_temp = wf.executeWorkflowSql("""select
	        * 
	    from
	        """ + m_SS_CCO_SALES_FACT_Demo_properties.Lkp_Date_Dim_DB_NAME + """.date_dim""", 'Lkp_Date_Dim_2', dbType=performance.resolveDBTypeForJDBCConnection(m_SS_CCO_SALES_FACT_Demo_properties.Lkp_Date_Dim_JDBC_DB_TYPE), databaseName=m_SS_CCO_SALES_FACT_Demo_properties.Lkp_Date_Dim_DB_NAME, targetTableName=None, isLoad=True)
	performance.performanceTuning(df_jdbc_temp, """{'databaseName' : '""" + INFORMATICA_DATABASE + """', 'typeOfInsert' : 'OVERRIDE', 'tableName' : 'DATE_DIM', 'queryId' : 'Lkp_Date_Dim_JDBC_TEMP'}""", False);
	df5 = wf.executeWorkflowSql("""SELECT
	        in0.UNIQUE_ID,
	        lkp.DT_KY AS DT_KY,
	        CAST (NULL AS STRING) DT_ID,
	        in0.SALES_DT AS SALES_DT  
	    FROM
	        Jnr_CCO_Sales_out_1_0 AS in0     
	    LEFT OUTER JOIN
	        (
	            """ + INFORMATICA_DATABASE + """.DATE_DIM
	        ) AS lkp 
	            ON lkp.DT_ID = in0.SALES_DT""", 'Lkp_Date_Dim_1');
	performance.performanceTuning(df5, """{'databaseName' : '""" + HIVE_PREPARATION_SCHEMA_NAME + """', 'typeOfInsert' : 'OVERRIDE', 'tableName' : 'Lkp_Date_Dim_out_1_0', 'queryId' : 'WM_5'}""", False);
	################################################################################################################################
	#	Informatica Enity Name : EXP_CCO_SALES_DATA
	#	Informatica Type : EXPRESSION
	#	Mapping Name : m_SS_CCO_SALES_FACT_Demo
	#	Comments and Information: Please go through the following points:
	# 	            1. The Number of Inputs For expression are : 2
	# 	            2. Number of I/O Present are : 10 
	# 	            3. Number of Running local Variable Present are : 0
	################################################################################################################################
	df6 = wf.executeWorkflowSql("""SELECT
	        in0.UNIQUE_ID,
	        lookup0.DT_KY AS DT_KY,
	        in0.CCO_ID AS CCO_ID,
	        in0.CCO_NAME AS CCO_NAME,
	        in0.o_CCO_SFID AS CCO_SFID,
	        in0.INVOICE_ID AS INVOICE_ID,
	        in0.PRODUCT_ID AS PRODUCT_ID,
	        in0.SALES_DT AS SALES_DT,
	        in0.UNIT_SOLD AS UNIT_SOLD,
	        in0.UNIT_PRICE AS UNIT_PRICE,
	        in0.TOT_AMT AS TOT_AMT  
	    FROM
	        Jnr_CCO_Sales_out_1_0 AS in0    
	    INNER JOIN
	        Lkp_Date_Dim_out_1_0 AS lookup0 
	            ON in0.unique_id = lookup0.unique_id""", 'Exp_CCO_Sales_data_1');
	performance.performanceTuning(df6, """{'databaseName' : '""" + HIVE_PREPARATION_SCHEMA_NAME + """', 'typeOfInsert' : 'OVERRIDE', 'tableName' : 'Exp_CCO_Sales_data_out_1_0', 'queryId' : 'WM_6'}""", False);
	################################################################################################################################
	#	Informatica Enity Name : CCO_SALES_FACT
	#	Informatica Type : TARGET
	#	Mapping Name : m_SS_CCO_SALES_FACT_Demo
	#	Target Properties: 
	# 	            1. Delete : NO
	# 	            2. Reject filename : Orcl_krog
	# 	            3. Update else Insert : NO
	# 	            4. Target load type : Normal
	# 	            5. Reject file directory : """ + m_SS_CCO_SALES_FACT_Demo_properties.PMBadFileDir + """\
	# 	            6. Insert : YES
	# 	            7. Update as Update : NO
	# 	            8. Truncate target table option : YES
	# 	            9. Update as Insert : NO
	################################################################################################################################
	INFORMATICA_DATABASE = m_SS_CCO_SALES_FACT_Demo_properties.tgt_database_CCO_SALES_FACT;
	performance.executeSql("use " + INFORMATICA_DATABASE);
	#performance.executeSql("""CREATE TABLE """ + INFORMATICA_DATABASE + """.CCO_SALES_FACT (CCO_ID DECIMAL(15,0),CCO_NAME string,CCO_SFID string,INV_ID DECIMAL(15,0),PROD_ID DECIMAL(15,0),SALES_DT_KY DECIMAL(15,0),UNIT_SOLD DECIMAL(15,0),UNIT_PRICE double,TOT_AMT double)""") 
	df7 = wf.executeWorkflowSql("""  SELECT
	in1.CCO_ID AS CCO_ID,
	in1.CCO_NAME AS CCO_NAME,
	in1.CCO_SFID AS CCO_SFID,
	in1.INVOICE_ID AS INV_ID,
	in1.PRODUCT_ID AS PROD_ID,
	in1.DT_KY AS SALES_DT_KY,
	in1.UNIT_SOLD AS UNIT_SOLD,
	in1.UNIT_PRICE AS UNIT_PRICE,
	in1.TOT_AMT AS TOT_AMT 
	FROM 
	Exp_CCO_Sales_data_out_1_0 AS in1""", 'CCO_SALES_FACT_1', dbType=performance.resolveDBTypeForJDBCConnection(m_SS_CCO_SALES_FACT_Demo_properties.CCO_SALES_FACT_JDBC_DB_TYPE), databaseName=m_SS_CCO_SALES_FACT_Demo_properties.CCO_SALES_FACT_DB_NAME, targetTableName='CCO_SALES_FACT', isLoad=False);

	return {}
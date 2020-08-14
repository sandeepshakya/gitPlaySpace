from pandas import *
from pyspark.sql import HiveContext, SparkSession
import pandas
import numpy as np
import math
import pkgutil
import csv
import os
import os.path
import ast
import sys
import InformaticaHouseKeeping
import RegisterUDF
import JdbcConfiguration
import Configure
import logging
import inspect

class Performance:
	isExecutionOnDatabricks = True
	isReadFromCSV = True
	xls_dictionary = {}
	dict_obj_global = {}
	dropTableList = list() 
	global_wm = "wm_global"
	previous_tableName = ''
	previous_databaseName = ''
	previous_queryId = ''
	recoveryQueryId = None
	recoveryTableName = None
	recoveryDatabaseName = None
	inputTables = ''
	recoveryMode = False
	mappingName = ''
	sessionName = ''
	taskInstanceName = ''
	recoveryMappingName = None
	dict_recovery_map = {}
	executedTargetIDList = []
	recoveryTargetQueryIDList = ""
	global IDW_INFORMATICA_HOME
	global INFORMATICA_MAPPING_PATH
	global INFORMATICA_MAPPLETS_PATH
	global INFORMATICA_STORED_ROCEDURE_PATH
	global TEMP_FILE_DIR
	global PARAMS_FILE_DIR
	global isProperyDictionaryPreprationDone
	global dictionaryForAllSessionProperties
	INFORMATICA_MAPPING_PATH = '/ConvertedCode/output/mappings/'
	INFORMATICA_MAPPLETS_PATH = '/ConvertedCode/output/mapplets/'
	INFORMATICA_STORED_ROCEDURE_PATH = '/ConvertedCode/output/storedProcedure/'
	INFORMATICA_WORKFLOW_PATH = '/ConvertedCode/output/workflows/'
	TEMP_FILE_DIR = '/tmp/'
	PARAMS_FILE_DIR = '/params/'
	isProperyDictionaryPreprationDone = False
	dictionaryForAllSessionProperties = {}
	sourceType = None
	targetTable = None
	isLoad = False
	originalDBName = None
	typeOfInsert = None
	
	if(os.environ.has_key('IDW_INFORMATICA_HOME')):
		IDW_INFORMATICA_HOME = os.environ['IDW_INFORMATICA_HOME']
	else:
		commons = os.path.dirname(os.getcwd())
		output = os.path.dirname(commons)
		convertedCode = os.path.dirname(output)
		IDW_INFORMATICA_HOME = os.path.dirname(convertedCode)

	def __init__(self, executionDirectory, workflowName):
		global workflowExecutionDirectory
		global dict_obj_global
		global global_wm
		global sqlContext
		global paramsDirectory
		global tempDirPath
		global IDW_INFORMATICA_HOME
		sqlContext = SparkSession.builder.appName("Informatica").config('spark.jars.packages','com.databricks:sparkcsv_2.10:1.0.3').enableHiveSupport().getOrCreate()
		workflowExecutionDirectory = executionDirectory
		if(self.isExecutionOnDatabricks):
			IDW_INFORMATICA_HOME = workflowExecutionDirectory
			os.environ['IDW_INFORMATICA_HOME'] = IDW_INFORMATICA_HOME
			tempDirPath = workflowExecutionDirectory + self.INFORMATICA_WORKFLOW_PATH + workflowName.strip() + TEMP_FILE_DIR
			# Get params folder path
			paramsDirectory = os.path.dirname(os.path.dirname(workflowExecutionDirectory + self.INFORMATICA_WORKFLOW_PATH)) + PARAMS_FILE_DIR
			print("****************paramsDirectory**********"+paramsDirectory)
		else:
			tempDirPath = workflowExecutionDirectory + TEMP_FILE_DIR
			# Get params folder path
			paramsDirectory = os.path.dirname(os.path.dirname(workflowExecutionDirectory)) + PARAMS_FILE_DIR
			
		spPath = IDW_INFORMATICA_HOME + INFORMATICA_STORED_ROCEDURE_PATH
		sys.path.append(spPath)
		# self.load_modules_from_path(IDW_INFORMATICA_HOME + '/lib/procedures/');
		xls_dictionary = {}
		dict_obj_global = {}
		self.removeRecoveryFile()
		self.setRecoveryMode()
		
	def setRecoveryMode(self):
		separator = "="
		print("*********************************************")
		file = os.getcwd() + '/recovery.properties'
		try:
			temp_dict = {}
			with open(file) as f:
				for line in f:
					if separator in line:
						name, value = line.split(separator, 1)
						dict_obj_global[name] = value.rstrip("\n\r")
			self.dict_recovery_map = dict_obj_global
			self.recoveryMode = True
			self.setvariablesFromRecoveryFile()
			self.removeRecoveryFile()
			print("Recovery Variables are Configured")
			#self.executeRecovery(sqlContext)
			print("*********************************************")
		except Exception as e:
			print("Property file not available : Skipping Recovery Mode")
			print(e)

	def setvariablesFromRecoveryFile(self):
		recovery_map = self.dict_recovery_map
		for key, value in recovery_map.items():
			if(key == 'mappingName'):
				self.recoveryMappingName = value
			elif(key == 'queryId'):
				self.recoveryQueryId = value.split("_")[1]
			elif(key == 'tableName'):
				self.recoveryTableName = value
			elif(key == 'databaseName'):
				self.recoveryDatabaseName = value
			elif(key == 'executedTargetQueryID'):
				self.recoveryTargetQueryIDList = value
	
	def executeSql(self, query):
		if(self.sourceType != None and (not query.lower().strip().startswith('use ')) and (self.sourceType != 'hive') and (self.sourceType != 'spark') and (self.sourceType != 'pyspark')):
			return self.executeJdbcSql(query, self.sourceType, self.originalDBName, self.targetTable, self.isLoad)
		else:
			if(self.executionFlag()):
				print("Executing the following query :\n" + query)
				df_temp = sqlContext.sql(query)
				return df_temp 
			else:
				print("Skipping the execution of the query :\n" + query)
		return None

	def executeJdbcSql(self, query, sourceType, originalDBName, dbTable, isLoad):
		if(self.executionFlag()):			
			sourceType = sourceType.lower().strip()
			jdbcUrl = self.getUrl(sourceType)
			driver = self.getDriver(sourceType)
			userName = self.getUser(sourceType)
			password = self.getPassword(sourceType)
			if(isLoad):
				print("Executing the following query on " + sourceType + " :\n" + query)
				jdbcDF = sqlContext.read.format("jdbc").option("url", jdbcUrl).option("user", userName).option("password", password).option("driver", driver).option("dbtable", "( " + query + " )  foo").load()
				return jdbcDF
			else:
				if(not "." in dbTable and originalDBName != None and originalDBName != ''):
					dbTable = originalDBName.strip() + "." + dbTable.strip()
				print("Executing the following query to prepare target data :\n" + query)
				df_temp = sqlContext.sql(query)
				print("Saving " + dbTable + " target table on " + sourceType + "." )
				if(self.typeOfInsert != None and self.typeOfInsert.strip() == "override"):
					df_temp.write.mode("override").format("jdbc").option("url", jdbcUrl).option("user", userName).option("password", password).option("driver", driver).option("truncate", True).option("dbtable", dbTable).save()
				else:
					df_temp.write.mode("append").format("jdbc").option("url", jdbcUrl).option("user", userName).option("password", password).option("driver", driver).option("dbtable", dbTable).save()
				return df_temp
		else:
			print("Skipping the execution of the query :\n" + query)
		return None
		
	# def executeSql(self, sqlContext, query, recoveryFlag):
		# try:
			# file = os.getcwd() + '/ExecutedQueries.txt'
			# file2 = os.getcwd() + '/SkippedQueries.txt'
			# if(recoveryFlag):
				# with open(file, "a") as myfile:
					# myfile.write(query + "\n\n")
					# myfile.write("*********************************************************************************************\n")
					# myfile.close()
			# else:
				# with open(file2, "a") as myfile:
					# myfile.write(query + "\n\n")
					# myfile.write("*********************************************************************************************\n")
					# myfile.close()
		# except:
			# print("FAILED TO WRITE")
		# if(recoveryFlag):
			# print("Executing the following query :\n" + query)
			# df_temp = sqlContext.sql(query)
			# return df_temp 
		# else:
			# print("Skipping the execution of the query :\n" + query)
		# return None

	def registerCustomUDFs(self, args):
		sys.path.append(IDW_INFORMATICA_HOME + '/ConvertedCode/output/commons/UDFs/')
		modulename = 'UDFs_' + args['folderName']
		obj = __import__(modulename, globals(), locals(), ['*'])
		obj.registerUDFs(self.getSqlContext())
		
	def setpreviousStageProperties(self, databaseName, tableName, queryId):
		self.previous_tableName = tableName
		self.previous_databaseName = databaseName
		self.previous_queryId = queryId
	
	#def performanceTuning(self, sqlContext, dataframe, databaseName, tableName, queryId, insertType, isTarget):
	def performanceTuning(self, dataframe, arguments, isTarget):
		
		global intermediatetempTableSet
		global dict_obj_global
		global argumentsMap

		databaseName = ""
		tableName = ""
		insertType = ""
		argumentsMap = ast.literal_eval(arguments)
		if 'databaseName' in argumentsMap:
			databaseName = argumentsMap["databaseName"]
		if 'tableName' in argumentsMap:
			tableName = argumentsMap["tableName"]
		if 'typeOfInsert' in argumentsMap:
			insertType = argumentsMap["typeOfInsert"]
		queryId = argumentsMap["queryId"]
		self.setpreviousStageProperties(databaseName, tableName, queryId)
		if(not isTarget):
			self.dropTableList.append(databaseName + "." + tableName)

		#If xls is present
		if(xls_dictionary.get(queryId) != None):
			#set input tables 
			recoveryQueryId = self.recoveryQueryId
			if(self.recoveryMode):
				if(self.recoveryMappingName != None and self.mappingName == self.recoveryMappingName):
					print("*****In the mapping where the execution failed in the previous run!*****")
					print("*****Setting the recovery flag to false*****")
					self.recoveryMode = False
					self.recoveryMappingName = None
					if(isTarget and queryId in self.recoveryTargetQueryIDList):
						if(tableName == ""):
							print("Skipping the flat file target : " + queryId)
						else:
							print("Skipping the target : " + databaseName + "." + tableName)
					else:
						self.execute_pre_post(dataframe, databaseName, tableName, queryId, insertType, isTarget)
				else:
					print("Recovery mode enabled. Not executing the following queryId : " + queryId)
			else:
				if(recoveryQueryId != None):
					if(isTarget and queryId in self.recoveryTargetQueryIDList):
						if(tableName == ""):
							print("Skipping the flat file target : " + queryId)
						else:
							print("Current query id is : " + queryId + " and the executed targets are : " + str(self.recoveryTargetQueryIDList))
							print("\nSkipping the target : " + databaseName + "." + tableName)
					else:
						self.execute_pre_post(dataframe, databaseName, tableName, queryId, insertType, isTarget)
				else:
					self.execute_pre_post(dataframe, databaseName, tableName, queryId, insertType, isTarget)
		else:
			print("XLS not present! Executing and saving all the tables.")
			targetType = ""
			if (isTarget) :
				targetType = argumentsMap["targetType"]
			if(targetType == "FlatFile"):
				self.saveFile(dataframe, argumentsMap)
			else:
				self.registerAndSaveTableInSpark(dataframe, databaseName, tableName, insertType, isTarget)

		if(isTarget) :
			self.addTargetQueryID(queryId)
		self.temp_table = tableName
		self.temp_database = databaseName
	
	def addTargetQueryID(self, queryId):
		self.executedTargetIDList.append(queryId)
		
	# def executeRecovery(self, sqlContext):
		# try:
			# tableName = self.previous_tableName
			# databaseName = self.previous_databaseName
			# queryId = self.previous_queryId
			# inputTable = self.inputTables
			# inputTables = inputTable.split(",")
			# for table in inputTables:	
				# print(table)
				# dataframe = sqlContext.sql("select * from " + databaseName + "." + table)
				# self.registerTableInSpark(dataframe, table)
		# except:
			# self.recoveryMode = False
		
	def execute_pre_post(self, dataframe, databaseName, tableName, queryId, insertType, isTarget):
		global intermediatetempTableSet
		global xls_dictionary
		global dict_obj_global
		#self.getInputTablesFromXLS(queryId)
		#Pre call
		self.pre_post_Tuning(queryId, "PRE", databaseName)
		dictionary_id = xls_dictionary.get(queryId)
		dictionary_main = dictionary_id.get("POST")
		
		if(isTarget and dataframe != None):
			targetType = argumentsMap["targetType"]
			if(targetType == "FlatFile"):
				self.saveFile(dataframe, argumentsMap)
			elif(targetType == "XML"):	
				self.saveXML(dataframe, argumentsMap)	
			else:
				if insertType.upper().strip() == 'OVERRIDE':
					# sqlContext.sql("drop table if exists temp_table")
					# dataframe.write.mode("overwrite").saveAsTable(databaseName + ".temp_table");
					# dataframe = sqlContext.table(databaseName + ".temp_table")
					dataframe.write.insertInto(databaseName + "." + tableName, overwrite = True)
				else:
					dataframe.write.insertInto(databaseName + "." + tableName, overwrite = False)
		else:
			if(dictionary_main != None):
				saveTable = dictionary_main.get("Save Table")
				if(saveTable != None and tableName != None and saveTable != 'None' and  saveTable != ''):
					if(saveTable.lower().strip() == "yes"):
						#dataframe.write.mode("overwrite").saveAsTable(databaseName + "." + tableName);
						self.registerAndSaveTableInSpark(dataframe, databaseName, tableName, insertType, isTarget)
					else:
						#Register table
						self.registerTableInSpark(dataframe, tableName)
				else:
					#Register table 
					self.registerTableInSpark(dataframe, tableName)
			else:
				#Register table 
				self.registerTableInSpark(dataframe, tableName)
			
			#dataframe.registerTempTable(tableName);
		#Post call
		self.pre_post_Tuning(queryId, "POST", databaseName)

	def registerAndSaveTableInSpark(self, dataframe, databaseName, tableName, insertType, isTarget):
		if(dataframe != None):
			if(isTarget):
				print("Writing to the target : " + databaseName + "." + tableName)
				if insertType.upper().strip() == 'OVERRIDE':
					dataframe.write.insertInto(databaseName + "." + tableName, overwrite = True)
				else:
					dataframe.write.insertInto(databaseName + "." + tableName, overwrite = False)
			else:
				print("Saving the table : " + databaseName + "." + tableName)
				if insertType.upper().strip() == 'OVERRIDE':
					dataframe.write.mode("overwrite").saveAsTable(databaseName + "." + tableName)
				elif insertType.upper().strip() == 'INSERT':
					dataframe.write.insertInto(databaseName + "." + tableName, overwrite = False)
				else:
					sqlContext.sql("Drop table if exists " + databaseName + "." + tableName)
					dataframe.write.saveAsTable(databaseName + "." + tableName);

				temp_dataframe = sqlContext.sql('select * from ' + databaseName + "." + tableName)
				temp_dataframe.registerTempTable(tableName);
	
	def registerTableInSpark(self, dataframe, tableName):
		if(dataframe != None):
			dataframe.registerTempTable(tableName);
		
	def global_Tuning(self):
		global dict_obj_global
		if(dict_obj_global != None):
			# AutoBroadcastJoinThreshold
			autoBroadcastJoinThreshold = str(dict_obj_global.get("AutoBroadcastJoinThreshold"))
			if(autoBroadcastJoinThreshold != 'None' and  autoBroadcastJoinThreshold != ''):	
				query="set spark.sql.autoBroadcastJoinThreshold=" + str(autoBroadcastJoinThreshold);
				self.executeSql(query)
			#ShufflePartitions
			ShufflePartitions = str(dict_obj_global.get("ShufflePartitions"))
			if(ShufflePartitions != 'None' and  ShufflePartitions != ''):	
				query = "set spark.sql.shuffle.partitions=" + str(ShufflePartitions);
				self.executeSql(query)
			#Cache Tables
			cacheTable = str(dict_obj_global.get("Cache Tables"))
			if(cacheTable != 'None' and cacheTable != ''):
				cacheTables = cacheTable.split(",")
				for tables in cacheTables:
					query = "CACHE TABLE " + tables
					self.executeSql(query)
			#UnCache Tables
			unCacheTable = str(dict_obj_global.get("UnCache Tables"))
			if(unCacheTable != None and unCacheTable != 'None' and unCacheTable != ''):
				unCacheTables = unCacheTable.split(",")
				for tables in unCacheTables:
					query="UNCACHE TABLE " + tables
					self.executeSql(query)
			#AdHoc Prperties
			addHocPrperty = str(dict_obj_global.get("Ad Hoc Propeties"))
			if(addHocPrperty != None and addHocPrperty != 'None' and addHocPrperty != ''):
				addHocPrperties = addHocPrperty.split(";")
				for queries in addHocPrperties:
					query = queries
					self.executeSql(query)
						
	
	def pre_post_Tuning(self, queryId, pre_post, databaseName):
		if(xls_dictionary.get(queryId) != None):
			dictionary_id = xls_dictionary.get(queryId)
			dictionary_pre_post = dictionary_id.get(pre_post)
			if(dictionary_pre_post != None):
				# AutoBroadcastJoinThreshold
				autoBroadcastJoinThreshold = str(dictionary_pre_post.get("AutoBroadcastJoinThreshold"))
				if(autoBroadcastJoinThreshold != None and autoBroadcastJoinThreshold != 'None' and  autoBroadcastJoinThreshold != ''):	
					if(autoBroadcastJoinThreshold.lower().strip() == self.global_wm):
						autoBroadcastJoinThreshold=getValueFromGlobal("AutoBroadcastJoinThreshold", autoBroadcastJoinThreshold)
					query = "set spark.sql.autoBroadcastJoinThreshold=" + str(autoBroadcastJoinThreshold);
					self.executeSql(query)
				#ShufflePartitions
				ShufflePartitions = str(dictionary_pre_post.get("ShufflePartitions"))
				if(ShufflePartitions != None and ShufflePartitions != 'None' and ShufflePartitions != ''):	
					if(ShufflePartitions.lower().strip() == self.global_wm):
						ShufflePartitions=getValueFromGlobal("ShufflePartitions", ShufflePartitions)
					query = "set spark.sql.shuffle.partitions=" + str(ShufflePartitions);
					self.executeSql(query)
				#Cache Tables
				cacheTable = str(dictionary_pre_post.get("Cache Tables"))
				if(cacheTable != None and cacheTable != 'None' and  cacheTable != ''):
					cacheTables = cacheTable.split(",")
					for tables in cacheTables:
						if(len(tables.split(".")) == 1):
							tables = databaseName + "." + tables
						query = "CACHE TABLE " + tables
						self.executeSql(query)
				#UnCache Tables
				unCacheTable = str(dictionary_pre_post.get("UnCache Tables"))
				if(unCacheTable != None and unCacheTable != 'None' and unCacheTable != ''):
					unCacheTables = unCacheTable.split(",")
					for tables in unCacheTables:
						if(len(tables.split(".")) == 1):
							tables = databaseName + "." + tables
						query = "UNCACHE TABLE " + tables
						self.executeSql(query)
				#AdHoc Prperties
				addHocPrperty = str(dictionary_pre_post.get("Ad Hoc Propeties"))
				if(addHocPrperty != None and addHocPrperty != 'None' and addHocPrperty != ''):
					if(addHocPrperties.lower().strip() == self.global_wm):
						addHocPrperties = getValueFromGlobal("Ad Hoc Propeties", addHocPrperties)
					addHocPrperties = addHocPrperty.split(";")
					for queries in addHocPrperties:
						query = queries
						self.executeSql(query)
			
	def dropIntermediateTable(self):
		#self.removeRecoveryFile()
		print("Dropping the intermediate tables :")
		self.setJDBCArgsNull();
		for dropTable in self.dropTableList:
			query = "DROP TABLE IF EXISTS " + dropTable
			self.executeSql(query)
	
	def removeRecoveryFile(self):
		file = os.getcwd() + '/recovery.properties'
		try:
			os.remove(file)
		except:
			print("Cannot delete : " + file)
	
	def getValueFromGlobal(self, nameOfProperty, value):
		global dict_obj_global
		if(dict_obj_global != null):
			temp = dict_obj_global.get(nameOfProperty)
			return temp
		return value
			
	def onFailure(self):
		self.writeToRecoveryFile(sqlContext)
			
	def writeToRecoveryFile(self, sqlContext):
		print("Writing to recovery file")
		tableName = self.previous_tableName
		databaseName = self.previous_databaseName
		queryId = self.previous_queryId
		#inputTable = self.inputTables
		mappingName = self.mappingName
		# for id in self.executedTargetIDList :
			# executedTargetQueryID = executedTargetQueryID + id + ","
		#self.executePreRecovery(sqlContext, tableName, inputTable, databaseName)		
		print("Table : " + databaseName + "." + tableName + "\nQueryId : " + queryId + "\nMappingName : " + mappingName)
		file = os.getcwd() + '/recovery.properties'
		fileName = open(file, "w")
		
		if(queryId == None or queryId == ""):
			fileName.write("#First query failed! Hence property values not set. Default queryId set to 0.\n")
			queryId = "WM_0"
		fileName.write("tableName=" + tableName + "\n")
		fileName.write("databaseName=" + databaseName + "\n")
		fileName.write("queryId=" + queryId + "\n")
		fileName.write("mappingName=" + mappingName + "\n")
		fileName.write("executedTargetQueryID=" + str(self.executedTargetIDList) + "\n")
		fileName.close()
		
	# def	executePreRecovery(self, sqlContext, tableName, inputTable, databaseName):
		# inputTables = inputTable.split(",")
		# status = True
		# for table in inputTables:
			# try:
				# query = "select * from " + table
				# print(query)
				# dataframe = sqlContext.sql(query)
				# try:
					# dataframe.write.mode("overwrite").saveAsTable(databaseName + "." + table);
				# except:
					# dataframe.write.mode("overwrite").insertInto(databaseName + "." + table);
			# except Exception, e:
				# self.recoveryMode = False
				# print str(e)
				# print("Not able to execute the Recovery on table : " + table)
		# try:	
			# query = "select * from " + tableName
			# print(query)
			# dataframe = sqlContext.sql(query)
			# try:
				# dataframe.write.mode("overwrite").saveAsTable(databaseName + "." + tableName);
				# print("CREATED**********************")
			# except:
				# dataframe.write.mode("overwrite").insertInto(databaseName + "." + tableName);
				# print("INSERTED**********************")
		# except Exception, e: 
			# print str(e)
			# print("Not able to execute the Recovery on Main table : " + tableName)
		
	def getInputTablesFromXLS(self, queryId):
		if(xls_dictionary.get(queryId) != None):
			print(queryId)
			dictionary_id = xls_dictionary.get(queryId)
			dictionary_pre_post = dictionary_id.get("PRE")
			print("reading from xls old input tables")
			if(dictionary_pre_post != None):
				# inputTablesNames
				inputTablesNames = str(dictionary_pre_post.get("inputTableNames"))
				print(dictionary_pre_post)
				if(inputTablesNames != None and inputTablesNames != 'None' and inputTablesNames != ''):
					self.inputTables = inputTablesNames
					
	def isnan(self, value):
		try:
			return math.isnan(float(value))
		except:
			return False
	
	def setMappingName(self, mappingName):
		self.mappingName = mappingName

	def setSessionName(self, sessionName):
		self.sessionName = sessionName

	def getSessionName(self):
		return self.sessionName
		
	def getSqlContext(self):
		return sqlContext

	def registerUDFs(self):
		RegisterUDF.register(sqlContext)
		
	def configure(self):
		Configure.configureSession(sqlContext)
		
	def executionFlag(self):
		if(self.recoveryMappingName == None):
			return True
		elif(self.recoveryMappingName == self.mappingName) :
			return True
		else:
			return False

	def load_modules_from_path(self, path):
		"""	
		Import all modules from the given directory	
		"""	
		# Check and fix the path
		if path[-1:] != '/':
			 path += '/'
		# Get a list of files in the directory, if the directory exists
		if not os.path.exists(path):
			raise OSError("Directory does not exist: %s" % path)
		# Add path to the system path
		sys.path.append(path)
		# Load all the files in path
		for f in os.listdir(path):
			 # Ignore anything that isn't a .py file
			 if len(f) > 3 and f[-3:] == '.py':
				modname = f[:-3]
				print("""modname name from system->""" + modname)
				# Import the module
				__import__(modname, globals(), locals(), ['*'])

	def load_class_from_name(self, fqcn):
		# Break apart fqcn to get module and classname	
		paths = fqcn.split('.')
		modulename = '.'.join(paths[:-1])
		classname = paths[-1]
		# Import the module
		print("""modulename->""" + modulename + """ Class name->""" + classname + """ Paths->""" + str(paths))
		__import__(modulename, globals(), locals(), ['*'])
		# Get the class
		cls = getattr(sys.modules[modulename], classname)
		print(sys.modules[modulename])
		# Check cls
		if not inspect.isclass(cls):
			raise TypeError("%s is not a class" % fqcn)
		# Return class
		return cls
			
	def executeProcedure(self, dfInput, argumentMap):
		try:
			args = ast.literal_eval(argumentMap)
			procName = args['name']
			queryId	= args['queryId']
			print('Inside executeProcedure')
			if(self.recoveryMode):
				if(self.executionFlag()):
					if(queryId in self.recoveryTargetQueryIDList):
						print('Skipping the Procedure Call : ' + queryId)
					else:
						# load the stored procedure class
						class_name = self.load_class_from_name(procName)
						# instantiate the stored procedure
						obj = class_name()
						return obj.execute(sqlContext, dfInput, self, args)
			else:
				# load the stored procedure class
				class_name = self.load_class_from_name(procName)
				# instantiate the  stored procedure
				obj = class_name()
				return obj.execute(sqlContext, dfInput, self, args)
			self.addTargetQueryID(queryId)
		except:
			raise
			self.onFailure()
			
	def initializeSession(self, argumentsMap):
		global xls_dictionary
		global taskInstanceName
		global folderName
		global IDW_INFORMATICA_HOME
		self.executedTargetIDList = []		
		sessionName = argumentsMap['sessionName']
		mappingName = argumentsMap['mappingName']
		folderName = argumentsMap['folderName']
		taskInstanceName = argumentsMap['taskInstanceName']
		self.setSessionName(sessionName)
		mappingPath = IDW_INFORMATICA_HOME + INFORMATICA_MAPPING_PATH + folderName + "/" + mappingName
		sys.path.append(mappingPath)
		extention = ".xlsx"
		if(self.isReadFromCSV):
			extention = ".csv"
		
		# If mappingXLS exists at the workflow directory, use this, else use the mappingXLS
		if(os.path.isfile(workflowExecutionDirectory + "/" + mappingName + extention)):
			fileNameExcel = workflowExecutionDirectory + "/" + mappingName + extention
		else:
			fileNameExcel = mappingPath + "/" + mappingName + extention
		xls_dictionary = {}
		dict_obj_global = {}
		
		try:
			itr = None
			if(self.isReadFromCSV):
				itr = csv.DictReader(open(fileNameExcel))
			else:
				xls = pandas.read_excel(fileNameExcel, na_filter = False)
				df = pandas.DataFrame(data = xls)
				itr = xls.iterrows()
			
			count = 0
			dictionary = {}
			for row in itr:
				if(row["PRE/POST"] == "GLOBAL"):
					dict_obj_global["TableName"] = row["TableName"]
					dict_obj_global["No. Of Outputs"] = row["No. Of Outputs"]
					dict_obj_global["Cache Tables"] = row["Cache Tables"]
					dict_obj_global["Uncache Tables"] = row["Uncache Tables"]
					dict_obj_global["Save Table"] = row["Save Table"]
					dict_obj_global["AutoBroadcastJoinThreshold"] = row["AutoBroadcastJoinThreshold"]
					dict_obj_global["ShufflePartitions"] = row["ShufflePartitions"]
					dict_obj_global["Ad Hoc Propeties"] = row["Ad Hoc Propeties"]
					dict_obj_global["Transformation Type"] = row["Transformation Type"]
					dict_obj_global["Transformation Name"] = row["Transformation Name"]
				
				if(row["PRE/POST"] == "PRE"):
					count = 1
					dict_obj_pre = {}
					dict_obj_pre["TableName"] = row["TableName"]
					dict_obj_pre["No. Of Outputs"] = row["No. Of Outputs"]
					dict_obj_pre["Cache Tables"] = row["Cache Tables"]
					dict_obj_pre["Uncache Tables"] = row["Uncache Tables"]
					dict_obj_pre["Save Table"] = row["Save Table"]
					dict_obj_pre["AutoBroadcastJoinThreshold"] = row["AutoBroadcastJoinThreshold"]
					dict_obj_pre["ShufflePartitions"] = row["ShufflePartitions"]
					dict_obj_pre["Ad Hoc Propeties"] = row["Ad Hoc Propeties"]
					dict_obj_pre["Transformation Type"] = row["Transformation Type"]
					dict_obj_pre["Transformation Name"] = row["Transformation Name"]
					dict_obj_pre["inputTableNames"] = row["inputTableNames"]
					dictionary["PRE"] = dict_obj_pre
				else:
					count = 0
					dict_obj_post = {}
					dict_obj_post["TableName"] = row["TableName"]
					dict_obj_post["No. Of Outputs"] = row["No. Of Outputs"]
					dict_obj_post["Cache Tables"] = row["Cache Tables"]
					dict_obj_post["Uncache Tables"] = row["Uncache Tables"]
					dict_obj_post["Save Table"] = row["Save Table"]
					dict_obj_post["AutoBroadcastJoinThreshold"] = row["AutoBroadcastJoinThreshold"]
					dict_obj_post["ShufflePartitions"] = row["ShufflePartitions"]
					dict_obj_post["Ad Hoc Propeties"] = row["Ad Hoc Propeties"]
					dict_obj_post["Transformation Type"] = row["Transformation Type"]
					dict_obj_post["Transformation Name"] = row["Transformation Name"]
					dict_obj_post["inputTableNames"] = row["inputTableNames"]
					dictionary["POST"] = dict_obj_post
				if(count == 0):
					xls_dictionary[row["ID"]] = dictionary		
					dictionary = {}
			self.global_Tuning()
		except Exception, e: 
			print str(e)
		
	def endSession(self, sessionName):
		self.setMappingName('')
		print("Session " + sessionName + " completed!")
		
	def executeMapplet(self, workflowObject, argumentMap):
		global mappletName
		try:
			mappletName = argumentMap['mappletName']
			folderName = argumentMap['folderName']
			self.updateMappletProperties(argumentMap['property'])
			mappletPath = IDW_INFORMATICA_HOME + INFORMATICA_MAPPLETS_PATH + folderName + "/" + mappletName
			sys.path.append(mappletPath)
			if(self.isExecutionOnDatabricks):
				mappletName = "output.mapplets." + folderName + "." + mappletName + "." + mappletName
			obj = self.importModule(mappletName)
			return obj.executeMapplet(workflowObject, argumentMap)
		except:
			print("\n*************Error in executing mapplet : " + mappletName + "*************\n")
			raise

	def importModule(self, moduleName):
		try:
			obj = __import__(moduleName, globals(), locals(), ['*'])
		except:
			print("\nUnable to load entity : " + moduleName)
			raise
		return obj
		
	def updateMappletProperties(self, propertyObj):
		# mappletName = propertyObj['mappletName']
		properties = dir(propertyObj)
		for key in properties:
			if mappletName in key:
				# variableName = key.split(mappletName + "_")[1]
				variableName = key.replace(mappletName + "_", "")
				setattr(propertyObj, variableName, getattr(propertyObj, key))
		
	def executeMapping(self, workflowObject, argumentMap):
		try:
			if not isProperyDictionaryPreprationDone:
				self.initializePropertiesDictionary(argumentMap)
	
			mappingName = argumentMap['mappingName']
			self.updateSessionProperties(argumentMap['property'])
			self.setMappingName(mappingName)			
			if(self.isExecutionOnDatabricks):
				mappingName = "output.mappings." + folderName + "." + mappingName + "." + mappingName
			if(self.recoveryMode):
				if(self.executionFlag()):
					obj = self.importModule(mappingName)
					return obj.executeMapping(workflowObject, argumentMap)
			else:
				obj = self.importModule(mappingName)
				return obj.executeMapping(workflowObject, argumentMap)

		except:
			print("\n*************Error in executing mapping : " + argumentMap['mappingName'] + "*************\n")
			raise

	def initializePropertiesDictionary(self, argumentMap):
		try:
			propertyFileName = argumentMap['property']
			paramFileName = propertyFileName.param_file_name
			wfName = propertyFileName.wf_name
			sessionNames = []
			try:
				sessionNames = propertyFileName.sessions_available_in_wf.split(",")
			except:
				print("\n*************Sessions not available.*************\n")
		
			# Create an empty property file: We will keep all properties in it which we have replaced from param file(s).
			if not os.path.exists(tempDirPath):
				print("\n*************Creating tmp dir: " + tempDirPath + ".*************\n")
				os.mkdir(tempDirPath)
			else:
				print("\n*************tmp folder is already available: " + tempDirPath + " *************\n")
			fileObj  = open(tempDirPath + propertyFileName.__name__ + ".py.temp" , 'w')
			fileObj.write("# Contains all properties which we have replaced from param file(s).\n")
		
			global dictionaryForAllSessionProperties
			dictionaryForAllSessionProperties = self.getPropertyDictionaryFromParamFile(paramFileName, sessionNames, wfName)
		
			global isProperyDictionaryPreprationDone
			isProperyDictionaryPreprationDone = True
		except:
			print("\n*************Error in preparing dictionary from param files.*************\n")
			
	def updateSessionProperties(self, propertyObj):
		properties = dir(propertyObj)
		base_name = os.path.splitext(os.path.basename(os.path.abspath(propertyObj.__file__)))[0]
		data = tempDirPath + base_name + ".py.temp"
		propertyFileObj  = open(data, 'a')
		propertiesDict = {}
		if taskInstanceName in dictionaryForAllSessionProperties:
			propertiesDict = dictionaryForAllSessionProperties[taskInstanceName]
		# else:
			# propertiesDict = dictionaryForAllSessionProperties["ONLY_WF_PARAMS"]

		for key in properties:
			if taskInstanceName in key:
				if(len(key.split(taskInstanceName + "_")) > 1):
					variableName = key.replace(taskInstanceName + "_", "")
					# Fetch property value from Dictionary(prepared from param files)
					if variableName in propertiesDict:
						value = propertiesDict.get(variableName)
						setattr(propertyObj, variableName, value)
						propertyFileObj.write(variableName + '=' + value + '\n')
					else:
						setattr(propertyObj, variableName, getattr(propertyObj, key))
			elif key in propertiesDict:
				value = propertiesDict.get(key)
				setattr(propertyObj, key, value)
				propertyFileObj.write(key + '=' + value + '\n')
				
	def executeCommand(self, arguments):
		try:
			argumentsMap = ast.literal_eval(arguments)
			commandContent = argumentsMap['commandContent']
			queryId = argumentsMap['queryId']
			recoveryStrategy = argumentsMap['Recovery Strategy']
			if(self.recoveryMode):
				if(self.executionFlag()):
					if(queryId in self.recoveryTargetQueryIDList):
						print('Skipping the Command : ' + queryId)
					else:
						print(commandContent)
						# commandResult = os.system(commandContent)
						# if(commandResult != 0) :
							# raise
			else:
				print(commandContent)
				# commandResult = os.system(commandContent)
				# if(commandResult != 0) :
					# raise
			self.addTargetQueryID(queryId)
		except:
			self.onFailure()
			print("Error in executing the command!")
			raise

	#performance.performTransaction({"columns" : "in0.a as name, in1.b as surname", "columnName" : "in0.xyz", "location" : "your_loc", "tableName" : tableName, "mode" : "append/overwrite", "isDelimited" : "YES", "delimiter" : ',', 'rowdelimiter' : '0', 'null_character' : '*', 'header' : 'False', 'escapeCharacter' : '', 'queryId' : 'WM_2'});
	def performTransaction(self, argumentsMap):
		columns = argumentsMap["columns"]
		queryId = argumentsMap["queryId"]
		columnName = argumentsMap["columnName"]
		tableName = argumentsMap["tableName"]
		path1 = argumentsMap["location"]
		mode1 = argumentsMap["mode"]
		header1 = argumentsMap["header"]
		delimiter = argumentsMap["delimiter"]
		dataframe = self.executeSql("select distinct " + columnName + " from " + tableName)
		if(self.recoveryMode):
			if(self.executionFlag()):
				queryId = argumentsMap['queryId']
				if(queryId in self.recoveryTargetQueryIDList):
					print('Skipping the Transaction : ' + queryId)
				else:
					for row in dataframe.rdd.collect():
						fileName = row[columnName]
						dataframe2 = self.executeSql("select " + columns + " from " + tableName + " where " + columnName + " = '" + str(fileName) + "'" )
						# dataframe2.saveAsTextFile(str(fileName))
						dataframe2.write.csv(path = path1, mode = mode1, header = header1, sep = delimiter)
		else:
			for row in dataframe.rdd.collect():
				fileName = row[columnName]
				dataframe2 = self.executeSql("select " + columns + " from " + tableName + " where " + columnName + " = '" + str(fileName) + "'" )
				# dataframe2.saveAsTextFile(str(fileName))
				dataframe2.write.csv(path = path1, mode = mode1, header = header1, sep = delimiter)

	def saveFile(self, dataframe, argumentsMap):
		# csv(path, mode=None, compression=None, sep=None, quote=None, escape=None, header=None, nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None, timestampFormat=None)
		path1 = argumentsMap["location"]
		mode1 = argumentsMap["mode"]
		header1 = argumentsMap["header"]
		delimiter = argumentsMap["delimiter"]
		isDelimited = argumentsMap["isDelimited"]
		if(self.recoveryMode):
			if(self.executionFlag()):
				queryId = argumentsMap['queryId']
				queryNumber = queryId.split("_")[1]
				if(float(queryNumber) >= float(self.recoveryQueryId)):
					# delimiter = delimiter if isDelimited.upper() == 'YES' else ' '
					# dataframe.coalesce(1).write.format('csv').option("emptyValue",'').option("nullValue", None).option("delimiter",delimiter).option("header",header1).mode(mode1).save(path1);
					if path1.endswith("prm"):
						file1 = path1.replace("file://", "")
						cols = dataframe.columns
						text_file = open(file1, "w")
						for row in dataframe.rdd.toLocalIterator():
							for col in cols:
								n = text_file.write(row[col])
							n = text_file.write("\n")
						text_file.close()
					else:
						dataframe.write.csv(path = path1, mode = mode1, header = header1, sep = delimiter)
		else:
			if path1.endswith("prm"):
				# dataframe.write.csv(path = path1, mode = mode1, header = header1, sep = delimiter, quote = "")
				#dataframe.write.mode(mode1).format("text").save(path1)
				#src_files = os.listdir(path1.replace("file://", ""))
				file1 = path1.replace("file://", "")
				cols = dataframe.columns
				text_file = open(file1, "w")
				for row in dataframe.rdd.toLocalIterator():
					for col in cols:
						n = text_file.write(row[col])
					n = text_file.write("\n")
				text_file.close()
			else:
				dataframe.write.csv(path = path1, mode = mode1, header = header1, sep = delimiter)


	# performance.performanceTuning(df7a, """{'mode' : 'overwrite', 'isDelimited' : 'YES', 'delimiter' : ',', 'header' : 'False', 'location' : '""" + m_CHANGE_GEAR_EXPORT_From_WUPG_properties.DirectoryPath_PROCESS_STATUS_EXCEPTIONS + """ + """ + m_CHANGE_GEAR_EXPORT_From_WUPG_properties.FileName_PROCESS_STATUS_EXCEPTIONS + """', 'targetType' : 'XML', 'rootTag' : 'OryxDataSet', 'rowTag' : 'IManagedResource', 'escapeCharacter' : '', 'queryId' : 'WM_17'}""", True);			
	def saveXML(self, dataframe, argumentsMap):	
		# csv(path, mode=None, compression=None, sep=None, quote=None, escape=None, header=None, nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None, timestampFormat=None)
		path1 = argumentsMap["location"]
		mode1 = argumentsMap["mode"]
		header1 = argumentsMap["header"]
		delimiter = argumentsMap["delimiter"]
		rootTag = argumentsMap["rootTag"]
		rowTag = argumentsMap["rowTag"]
		if(self.recoveryMode):
			if(self.executionFlag()):
				queryId = argumentsMap['queryId']
				if(queryId in self.recoveryTargetQueryIDList):
					print('Skipping the XML Target : ' + queryId)
				else:
					# dataframe.write.csv(path = path1, mode = mode1, header = header1, sep = delimiter)
					dataframe.write.format("com.databricks.spark.xml").option("rootTag", rootTag).option("rowTag", rowTag).mode(mode1).save(path1)
		else:	
			# dataframe.write.csv(path = path1, mode = mode1, header = header1, sep = delimiter)	
			dataframe.write.format("com.databricks.spark.xml").option("rootTag", rootTag).option("rowTag", rowTag).mode(mode1).save(path1)	
						
	def send_email(self, argumentsMap):
		try:
			queryId = argumentsMap['queryId']
			emailUserName = argumentsMap['Email User Name']
			emailSubject = argumentsMap['Email Subject']
			emailText = argumentsMap['Email Text']
			if(self.recoveryMode):
				if(self.executionFlag()):
					if(queryId in self.recoveryTargetQueryIDList):
						print('Skipping the Email : ' + queryId)
					else:
						InformaticaHouseKeeping.send_email(emailUserName, emailSubject, emailText)
			else:
				InformaticaHouseKeeping.send_email(emailUserName, emailSubject, emailText)
			self.addTargetQueryID(queryId)
		except:
			this.onFailure()
			raise
			
	def getSubDictionaries(self, isFile, dir, entries, globalDict, wfDict, tasksParam, taskNames, workflowName):
		if dir != '' and not isFile:
			entries = os.listdir(dir)
		else:
			fileName = dir
			found = False
			for root, dirs, files in os.walk(paramsDirectory):
				if fileName in files:					
					dir = root
					found = True
					print("Fetching properties from " + root + "/" + fileName)
			
			# If not found then copy/find properties in all files
			if not found:
				print("File " + dir + " Not found in param dir hence fetching properties from all files.")
				dir = paramsDirectory
				entries = os.listdir(dir)
				
		for paramFilePath in entries:
			paramFilePath = dir + "/" + paramFilePath
			if(os.path.isdir(paramFilePath)):
				self.getSubDictionaries(False, paramFilePath, entries, globalDict, wfDict, tasksParam, taskNames, workflowName)
			else:
				with open(paramFilePath, 'r') as searchfile:			
					for line in searchfile:
						while(True):
							isLineForwarded = False;
							if 'GLOBAL' in line.upper() and line.strip().startswith('['):
								line = line.replace('[','').replace(']','').upper().strip();
								if(line == 'GLOBAL'):
									for line in searchfile :
										if('=' in line and '' in line.strip() and not line.strip().startswith('[')):
											arr = line.split('=',1)
											#if arr[0].replace('$','').strip() not in taskDict and arr[0].replace('$','').strip() not in wfDict:
											globalDict[arr[0].replace('$','').strip()] = arr[1].strip()
										else:
											isLineForwarded = True
											break
							
							if workflowName.upper() in line.upper() and line.strip().startswith('['):
								line = line.replace('[','').replace(']','').upper().strip();
								arr = line.split(':')
								if(len(arr) > 0 and arr[len(arr)-1].upper() == workflowName.upper()):
									for line in searchfile :
										if('=' in line and '' in line.strip() and not line.strip().startswith('[')):
											arr = line.split('=',1)
											wfDict[arr[0].replace('$','').strip()] = arr[1].strip()
										else:
											isLineForwarded = True
											break
											
							if line.strip().startswith('['):
								line = line.replace('[','').replace(']','').strip();
								arr = line.split('.')
								index = -1
								if(len(arr) > 0):
									try:
										index = taskNames.index(arr[len(arr)-1].strip())
									except:
										index = -1
									
									if(index >= 0):
										taskDict = {}
										for line in searchfile :
											if('=' in line and '' in line.strip() and not line.strip().startswith('[')):
												arr = line.split('=',1)
												taskDict[arr[0].replace('$','').strip()] = arr[1].strip()
											else:
												isLineForwarded = True
												break
										currentDict = tasksParam.pop(index)
										taskDict.update(currentDict)
										tasksParam.insert(index,taskDict)								
							if(not isLineForwarded):
								break
		return globalDict, wfDict, tasksParam;
			
	def getPropertyDictionaryFromParamFile(self, paramFileName, taskNames, workflowName):
		finalResult = {}
		tasksParam = []
		globalDict = {}
		wfDict = {}
		entries = []
		isFile = False
		
		# Check wf name is supplied or not, if not then return empty dictionary
		if workflowName == '' or workflowName == None:
			return {}
		
		if paramFileName == '' or paramFileName == None:
			entries = os.listdir(paramsDirectory)
			paramFileName = ''
		else:
			isFile = True
			entries.insert(0,paramFileName)

		# Initialize an empty array for session/task parames size
		index = 0;
		for x in taskNames:		
			tasksParam.append({});
			# Check task names are supplied or not, if not then prepare ONLY_WF_PARAMS dictionary
			if taskNames[index] == '' or taskNames[index] == None:
				taskNames[index] = 'ONLY_WF_PARAMS'
			index = index + 1
			
		globalDict,wfDict,tasksParam = self.getSubDictionaries(isFile, paramFileName,entries,globalDict,wfDict,tasksParam,taskNames,workflowName)

		# Merge all Dictionaries into assignment order of properties prefrences.
		index = 0
		for x in tasksParam:
			finalDict = {}
			finalDict.update(globalDict)
			finalDict.update(wfDict)
			finalDict.update(x)
			finalResult[taskNames[index]] = finalDict
			index = index + 1
		
		print('Property dictionary preparation done.')
		return finalResult;
		
	def setJDBCArgs(self, param):
		if param != None and len(param) > 0:
			for i, k in param.items():
				if(i == 'dbType'):
					allowedJDBC = getattr(JdbcConfiguration, "allowed_jdbc_sources")
					if(k != None and ('|' + k.lower().strip() + '|') in allowedJDBC):
						self.sourceType = k.lower().strip()
					else:
						self.sourceType = None
				if(k != None and i == 'targetTableName'):
					self.targetTable = k.lower().strip()
				if(i == 'isLoad'):
					self.isLoad = k
				if(i == 'databaseName'):
					self.originalDBName = k.lower().strip()
				if(i == 'typeOfInsert'):
					self.typeOfInsert = k.lower().strip()
		else:
			self.setJDBCArgsNull();
		
	def setJDBCArgsNull(self):
		self.sourceType = None
		self.targetTable = None
		self.isLoad = False
		self.originalDBName = None
		self.typeOfInsert = None
	
	def resolveDBTypeForJDBCConnection(self, dbType):
		try:
			dbType = getattr(JdbcConfiguration, dbType.strip() +  '_dbtype_to')
		except Exception as e:
			print('Error: DBType resolver : ' + dbType.strip() + '_dbtype_to is not present in JdbcConfiguration.py. Setting default dbType as HIVE.')
			dbType = 'hive'
		return dbType
	
	def getUrl(self, sourceType):
		url = getattr(JdbcConfiguration, sourceType + "_db_jdbc_url");
		user = getattr(JdbcConfiguration, sourceType + "_db_username");
		password = getattr(JdbcConfiguration, sourceType + "_db_password");
		host = getattr(JdbcConfiguration, sourceType + "_db_host");
		port = getattr(JdbcConfiguration, sourceType + "_db_port");
		databaseName = getattr(JdbcConfiguration, sourceType + "_db_dbname");
		if(self.originalDBName != None and self.originalDBName != '' and sourceType != "synapse" and sourceType != "oracle" and not "." in self.originalDBName):
			databaseName = self.originalDBName;
		url = url.replace('<user>', user).replace('<password>', password).replace('<host>', host).replace('<port>', port).replace('<dbName>', databaseName)
		return url
		
	def getUser(self, sourceType):
		user = sourceType + "_db_username";		
		return getattr(JdbcConfiguration, user);
		
	def getPassword(self, sourceType):
		password = sourceType + "_db_password";		
		return getattr(JdbcConfiguration, password);
		
	def getDriver(self, sourceType):
		driver = sourceType + "_db_driver";		
		return getattr(JdbcConfiguration, driver);
	
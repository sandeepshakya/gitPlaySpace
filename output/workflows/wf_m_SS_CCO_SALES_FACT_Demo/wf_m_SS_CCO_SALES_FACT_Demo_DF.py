from datetime import datetime
from pyspark.sql.types import *
import logging
import sys
import os
import wf_m_SS_CCO_SALES_FACT_Demo_properties

class InformaticaClass:
	performance =''
	global overriddenTransformationsList
	overriddenTransformationsList = ['SQ_CCO_1_s_m_SS_CCO_SALES_FACT_Demo']

	def executeWorkflowSql(self, query, transformationName, **param):
		global queryFromMapping
		queryFromMapping = query
		transformationName = transformationName + "_" + performance.getSessionName()
		# Set query args in case of jdbc connection.
		performance.setJDBCArgs(param);
		if transformationName in overriddenTransformationsList :
			#Execute transformation writtten in workflow
			print('-------------Executing session overriden query-------------')
			transformationMethod = getattr(self, transformationName);
			return transformationMethod();
		else:
			#Execute the mapping transformation
			print('-------------Executing mapping query-------------')
			return performance.executeSql(query)

	def __init__(self, performanceVariable):
		global performance
		performance = performanceVariable
		performance.configure()
		performance.registerUDFs()


	def execute(self):
		HIVE_PREPARATION_SCHEMA_NAME = wf_m_SS_CCO_SALES_FACT_Demo_properties.HIVE_PREPARATION_SCHEMA_NAME
		HIVE_LANDING_SCHEMA_NAME = wf_m_SS_CCO_SALES_FACT_Demo_properties.HIVE_LANDING_SCHEMA_NAME
		HIVE_INSIGHTS_SCHEMA_NAME = wf_m_SS_CCO_SALES_FACT_Demo_properties.HIVE_INSIGHTS_SCHEMA_NAME

		self.executeSession_s_m_SS_CCO_SALES_FACT_Demo()

		performance.dropIntermediateTable()
		print("*********************************SUCCESS!!*********************************")

	################################################################################################################################
	#	Informatica Enity Name : SQ_CCO
	#	Informatica Type : SOURCE QUALIFIER
	#	Mapping Name : m_SS_CCO_SALES_FACT_Demo
	################################################################################################################################
	def SQ_CCO_1_s_m_SS_CCO_SALES_FACT_Demo(self):
		df = performance.executeSql(""" SELECT
		    ROW_NUMBER() OVER (
		  ORDER BY
		    (SELECT
		      NULL)) as UNIQUE_ID,
		    CCO_ID AS CCO_ID,
		    CCO_NAME AS CCO_NAME,
		    CCO_SFID AS CCO_SFID 
		  FROM
		    """ + wf_m_SS_CCO_SALES_FACT_Demo_properties.s_m_SS_CCO_SALES_FACT_Demo_SQ_CCO + """.cco 
		  WHERE
		    ACTV_FL='Y' 
		    AND START_DT IS NOT NULL""");
		return df


	def executeSession_s_m_SS_CCO_SALES_FACT_Demo(self):
		print("Session s_m_SS_CCO_SALES_FACT_Demo Started From Here-------------------------------------------")
		performance.initializeSession({'sessionName' : 's_m_SS_CCO_SALES_FACT_Demo', 'taskInstanceName' : 's_m_SS_CCO_SALES_FACT_Demo', 'mappingName' : 'm_SS_CCO_SALES_FACT_Demo', 'folderName' : 'IA_Demo'})
		session_start_time = datetime.now()
		performance.executeMapping(self, {'mappingName' : 'm_SS_CCO_SALES_FACT_Demo', 'property' : wf_m_SS_CCO_SALES_FACT_Demo_properties, 'performanceObject' : performance})
		session_completion_time = datetime.now()
		performance.endSession('s_m_SS_CCO_SALES_FACT_Demo')
		performance.endSession('s_m_SS_CCO_SALES_FACT_Demo')
		print("Session s_m_SS_CCO_SALES_FACT_Demo Completed Here-------------------------------------------")


def main():
	workflowDirectoryPath = os.getcwd()
	performance = Performance(workflowDirectoryPath, 'wf_m_SS_CCO_SALES_FACT_Demo')
	m = InformaticaClass(performance)
	m.execute()
	
def execute(performance):
	m = InformaticaClass(performance)
	m.execute()


if __name__ == '__main__':
	global performance
	global IDW_INFORMATICA_HOME
	global INFORMATICA_COMMONS_PATH
	workflows = os.path.dirname(os.getcwd())
	output = os.path.dirname(workflows)
	commons = output + "/commons/"
	sys.path.append(commons)
	from Performance import *
	main()

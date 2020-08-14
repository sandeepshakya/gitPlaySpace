# JDBC connection required properties
allowed_jdbc_sources="|microsoftsqlserver|oracle|db2|synapse|"

# Database Types mapping (InformaticaDBType -> MigratedTo -> CloudDBType)
netezza_dbtype_to="synapse"
microsoftsqlserver_dbtype_to="microsoftsqlserver"
oracle_dbtype_to="synapse"
db2_dbtype_to="db2"

# microsoftsqlserver jdbc information
microsoftsqlserver_db_jdbc_url="jdbc:sqlserver://<host>:<port>;database=<dbName>;user=<user>;password=<password>"
microsoftsqlserver_db_username="bankunited"
microsoftsqlserver_db_password="impetus1234"
microsoftsqlserver_db_host="database-2.cgypfaem1nqt.us-east-1.rds.amazonaws.com"
microsoftsqlserver_db_port="1433"
microsoftsqlserver_db_dbname="azhar"
microsoftsqlserver_db_driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"

# Synapse jdbc information
synapse_db_jdbc_url="jdbc:sqlserver://<host>:<port>;database=<dbName>;user=<user>;password=<password>;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
synapse_db_username="sqladmin@sqlserverkroger"
synapse_db_password="Krog$12345"
synapse_db_host="sqlserverkroger.database.windows.net"
synapse_db_port="1433"
synapse_db_dbname="sqldw-synapse-krog"
synapse_db_driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"

# Oracle jdbc information
oracle_db_jdbc_url="jdbc:oracle:thin:@<host>:<port>:<dbName>"
oracle_db_username="bankunited"
oracle_db_password="impetus1234"
oracle_db_host="orcl.cgypfaem1nqt.us-east-1.rds.amazonaws.com"
oracle_db_port="1521"
oracle_db_dbname="ORCL"
oracle_db_driver="oracle.jdbc.driver.OracleDriver"

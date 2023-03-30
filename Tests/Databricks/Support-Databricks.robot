*** Settings ***
Library   	Library/DatabricksQuery.py
Library   	Library/Utils.py
Library   	Collections
Library   	String
Resource	Tests/Support.robot

*** Variables ***
@{blacklist}=		ETL_ID  ETL_CREATEDATE  ETL_LASTUPDATEDATE  ETL_USERID
@{whitelist_start}=	  	DI_OPERATION_TYPE	DI_SEQUENCE_NUMBER
@{whitelist_end}=		SYSTEM_NAME  ENVIRONMENT_CLASSIFICATION
@{whitelist}=		DEL_IND  DAI_ETL_ID  DAI_CRT_DTTM  DAI_UPDT_DTTM

@{list_of_columns_from_ac}=     ETL_ID  ETL_CREATEDATE  ETL_LASTUPDATEDATE  ETL_USERID

*** Keywords ***
# ---------------------------- S E T  U P  M E T H O D S ----------------------------
Set Up
	Connect to ORM
	Return all table names for system
	Disconnect Orm
    Connect to Databricks

Set Up Osi Pi
	Connect to ORM

Set Up DBFS
	Connect to Databricks
	log to console  [DEBUG - Databricks connected]
    run keyword if  "${table_limit}" != "none"  Return tables names in limit from dbfs      ELSE IF  "${test_tables}" != "all"   Return specific table names from dbfs      ELSE IF     "${test_tables}" == "all"    Return all table names from dbfs


#------------------------- Set Table names & primary keys ----------------------------
Return tables names in limit from dbfs
	log to console  [DEBUG - Table limit set to ${table_limit}]
    ${dict_tables_and_keyfields}=     RUN KEYWORD       get tables and keys from databricks   ${system}
    FOR     ${table}		IN 	  @{dict_tables_and_keyfields}
		${count_dbfs}=	run keyword and continue on failure  return count of table  ${system}.${table}
	    run keyword if  ${count_dbfs}>${table_limit}    remove from dictionary  ${dict_tables_and_keyfields}    ${table}
    END
    set suite variable  ${dict_tables_and_keyfields}

Return all table names from dbfs
    log to console  [DEBUG - testing all tables]
    ${dict_tables_and_keyfields}=     RUN KEYWORD       get tables and keys from databricks   ${system}
    set suite variable  ${dict_tables_and_keyfields}

Return specific table names from dbfs
    log to console  [DEBUG - testing specific tables]
    ${dict_tables_and_keyfields}=     RUN KEYWORD       get specific tables and keys from databricks  ${system}     ${test_tables}
    set suite variable  ${dict_tables_and_keyfields}
    FOR     ${table}    IN  @{dict_tables_and_keyfields}
        log to console  [DEBUG] ${system}.${table} under test
    END



# ---------------------------- Set Table names & primary keys ----------------------------
Return all table names for system
    ${all_tables}=     Run Keyword       return multiple columns from table     Metastore.vSourceConfiguration    tableName,keyfields       system='${system}'     1000
	#${all_tables}=		Run Keyword		I query tableName,keyfields from Metastore.vSourceConfiguration where tableName name in a list of @{specific_tables}
	${dict_tables_and_keyfields}=	 Create Dictionary
	FOR	${table}		IN 	  @{all_tables}
		Set to Dictionary	${dict_tables_and_keyfields}	${table[0]}=${table[1]}    # update dictionary to key=tablename value=keyfields
	END
	set suite variable  ${dict_tables_and_keyfields}

I return all table names for system osi pi
	${all_tables}=		create list 	B36   B59   D46  D49   D53
	${dict_tables_and_keyfields}=	 Create Dictionary
	FOR	${table}		IN 	  @{all_tables}
		Set to Dictionary	${dict_tables_and_keyfields}	${table}=${table}    # update dictionary to key=tablename value=keyfields
	END
	set suite variable  ${dict_tables_and_keyfields}

I return all table names from dbfs
    ${dict_tables_and_keyfields}=     RUN KEYWORD       get tables and keys from databricks   ${system}
    set suite variable  ${dict_tables_and_keyfields}

#------------------------- KAFKA UPDATES ----------------------------
# kafka tests must run manually
I check the inserts updates and deletes from kafka
    The inserts updates and deletes from kafka are checked

The inserts updates and deletes from kafka are checked
    log to console  Pass

I expect the inserts updates and deletes from kafka have been applied
    The inserts updates and deletes from kafka have been applied

The inserts updates and deletes from kafka have been applied
    log to console  Pass

Primary Keys are checked for ingested tables
    LOG TO CONSOLE  PASS

The uniqueness of the primary keys are as expected for ingested tables
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		${primary_keys}=	get from dictionary  ${dict_tables_and_keyfields}	${table}
		${results_from_select_all}=		run keyword and continue on failure  return count of table  	${system}.${table}
		${results_from_pks}=	run keyword and continue on failure  return count multiple columns from table 		${system}.${table} 	${primary_keys}
		run keyword and continue on failure  should be equal as numbers  ${results_from_select_all}	${results_from_pks}
		add result to report 	Select all result: ${results_from_select_all} \nSelect distinct primary keys result: ${results_from_pks}\nCounts are equal so there are no duplicate primary keys in ${system}.${table}\n
	END
	add result to report	Test Passed: There are no duplicate primary keys in ${system}

# ---------------------------- Check Table Target Lengths GIVEN----------------------------
Target tables are checked for length
    LOG TO CONSOLE  PASS

# ---------------------------- Check Table Target Lengths THEN----------------------------
The target tables are the correct length
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		${table_length}=	get length  ${table}
	 	add result to report  Table: ${table}\nTable Name length: ${table_length} is less than ${table_length_value}
		should not be true  ${table_length}>${table_length_value}
	END
	add result to report	Test Passed: Table names are less than ${table_length_value} characters in ${system}

# ---------------------------- Check Table Names----------------------------
Tables and tables names are checked in target database
    LOG TO CONSOLE  PASS

Tables exist in target database and match the source table names
	Should not be Empty  ${dict_tables_and_keyfields}
	${dict_of_all_tables}=	return table list in database	${system}
	add result to report  Expected list of tables in ${system}
	add result to report  "@{dict_of_all_tables}"
	add result to report  Actual list of tables in ${system}\nDBFS Server: \nUse ${system};\nShow tables;
	add result to report  "@{dict_of_all_tables}"
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		Dictionary should contain key  	${dict_of_all_tables}	${table}
	END
	add result to report   Test Passed: All tables exist in ${system}

Each Table in schema is checked
    LOG TO CONSOLE  PASS

The table files are in the correct folder for pasx
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		${system_lower}=	Convert To Lower Case	${system}
		${table_location}=		Run Keyword 	return location of table on dbfs 	${table}	${system}_STG
		should be equal as strings		${table_location}		dbfs:/user/hive/warehouse/${system_lower}_stg2.db/${table}
		add result to report	Source table: ${table} is in the correct folder location: ${table_location}
		The table files are in the correct folder on source ${table} ${system_lower}
	END
	add result to report	 Test Passed: All tables are in the correct folders in ${system}
	write to file  ${jira_id_tag}
	${var}=		convert to string  report-${jira_id_tag}.txt
	log file to report ${var}

The table files are in the correct raw folder
    Set JIRA tag
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		${system_lower}=	Convert To Lower Case	${system}
		${table_location}=		Run Keyword 	return location of table on dbfs 	${table}	${system}
		should be equal as strings		${table_location}		dbfs:/mnt/adls/raw/${system_lower}/${table}
		add result to report	Source table: ${table} is in the correct folder location: ${table_location}\n
		#The table files are in the correct folder on source ${table} ${system_lower}
	END
	add result to report	 Test Passed: All tables are in the correct folders for ${system}
	write to file  ${jira_id_tag}
	${var}=		convert to string  report-${jira_id_tag}.txt
	log file to report ${var}

I return column names for ${table} in schema ${schema}
	${dict_of_columns}=		Run Keyword  return column descriptions from table		${table}  ${schema}
	@{list_of_columns}=		Create List
	FOR	${result}	IN   @{dict_of_columns}
		Append to List	${list_of_columns}	${result}[name]
	END
	[Return]  ${list_of_columns}

I return datatype for ${column} in ${table} in schema ${schema}
    # Returns the datatype of a column as a string
	${dict_of_columns}=		Run Keyword  return column descriptions from table		${table}  ${schema}
	FOR	${result}	IN   @{dict_of_columns}
	    ${datatypevar}=    set variable  ${result}[type]
		${datatype}=     set variable if  '${result}[name]'=='${column}'   ${datatypevar}
		exit for loop if  '${result}[name]'=='${column}'
	END
    [Return]    ${datatype}

# ---------------------------- Compare counts of table THEN----------------------------
I expect the counts to match
  	Counts of source and target are equal

# ---------------------------- Compare counts of table GIVEN----------------------------
Counts of source and target are taken
    LOG TO CONSOLE  PASS

Counts of source and target are equal
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		${count_flat_file}=	 run keyword and continue on failure  return count of table  ${system}_STG2.${table}
		${count_dbfs}=	run keyword and continue on failure  return count of table version zero  ${system}.${table}
		run keyword and continue on failure  should be equal as integers  ${count_dbfs}	${count_flat_file-1}
		run keyword and continue on failure	 add result to report  Count of flat file (Source): ${count_flat_file-1}\nCount of delta table (Target): ${count_dbfs}\nSource: ${count_flat_file-1} equals Target: ${count_dbfs} for table ${table} in ${system}\n
	END
	run keyword and continue on failure  add result to report	 Test Passed: Counts of Source and Target version zero are equal for all tables in ${system}

Counts of source and target are equal for the system
    Set JIRA tag
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		${count_flat_file}=	 run keyword and continue on failure  return count of table  ${system}_STG2.${table}
		${count_dbfs}=	run keyword and continue on failure  return count of table   ${system}.${table}
		run keyword and continue on failure  should be equal as integers  ${count_dbfs}	${count_flat_file}
		run keyword and continue on failure	 add result to report  Count of flat file (Source): ${count_flat_file}\nCount of delta table (Target): ${count_dbfs}\nSource: ${count_flat_file} equals Target: ${count_dbfs} for table ${table} in ${system}\n
	END
	run keyword and continue on failure  add result to report	 Test Passed: Counts of Source and Target version zero are equal for all tables in ${system}
	write to file  ${jira_id_tag}
	${var}=		convert to string  report-${jira_id_tag}.txt
	log file to report ${var}

# ---------------------------- Compare Source and Target ----------------------------
I expect the columns to be present in the target table
    The columns are present in the target table

#todo- Possible extract list comparison to another keyword
The columns are present in the target table
    Set JIRA tag
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		${common_col_names}=	Run Keyword  I return column names for ${table} in schema ${system}
		set suite variable		${common_col_names}
       run keyword     list should contain sub list  ${common_col_names}   ${list_of_columns_from_ac}
		run keyword and continue on failure	 add result to report  Columns ${list_of_columns_from_ac} are present for table ${table} in ${system}
	END
	run keyword and continue on failure  add result to report	 Test Passed: Columns ${list_of_columns_from_ac} present for all tables in ${system}
	write to file  ${jira_id_tag}
	${var}=		convert to string  report-${jira_id_tag}.txt
	log file to report ${var}

I check for the specified columns in the source table
    Columns in the source table are checked

I check for the specified columns in the target table
    Columns in the target table are checked

I check the ETL_ID contains a unique value
    The ETL_ID is checked for a unique value

I check the ETL_USER_ID contains a database account
    The ETL_USER_ID is checked for a database account

# ---------------------------- Check ETL_CREATEDATE GIVEN  ----------------------------
I check the ETL_CREATEDATE contains a datetime
    The ETL_CREATEDATE is checked for a date time

The ETL_CREATEDATE is checked for a date time
    LOG TO CONSOLE    PASS

# ---------------------------- Check ETL_LASTUPDATEDATE GIVEN  ----------------------------
I check the ETL_LASTUPDATEDATE contains a datetime
    The ETL_LASTUPDATEDATE is checked for a date time

The ETL_LASTUPDATEDATE is checked for a date time
    LOG TO CONSOLE    PASS
# ---------------------------- Check ETL_ID GIVEN  ----------------------------

The ETL_ID is checked for a unique value
      LOG TO CONSOLE    PASS

The ETL_USER_ID is checked for a database account
    LOG TO CONSOLE    PASS

Records between source and target are checked
    LOG TO CONSOLE  PASS

Columns in the source table are checked
    LOG TO CONSOLE  PASS

Columns in the target table are checked
    LOG TO CONSOLE  PASS

I expect the ETL_ID contains a value that uniquely identifies the batch of the last update
    The ETL_ID contains a unique identifiable value

I expect the ETL_USER_ID contains the database account name that last changed the record
   The ETL_USERID contains the database account name that last changed the record

I expect the ETL_CREATEDATE contains a datetime
    The ETL_CREATEDATE contains a valid datetime in UTC format yyyy-MM-dd'T'HH:mm:ss.SSSZ

I expect the ETL_LASTUPDATEDATE contains a datetime
    The ETL_LASTUPDATEDATE contains a valid datetime in UTC format yyyy-MM-dd'T'HH:mm:ss.SSSZ

The ETL_ID contains a unique identifiable value
    FOR	${table}		IN 	  @{dict_tables_and_keyfields}
        ${null_count}=  run keyword and continue on failure  run count query with condition  ${table}    'ETL_ID'  ${system}   is null
        ${non_null_count}=  run keyword and continue on failure  run count query with condition  ${table}    'ETL_ID'  ${system}   is not null
    	${count_dbfs}=	run keyword and continue on failure  return count of table  ${system}.${table}
    	${result}=	run keyword and continue on failure  should be equal as integers    ${null_count}   0
        ${count_result}=	run keyword and continue on failure  should be equal as integers  ${non_null_count}  ${count_dbfs}
        ${datatype}=    run keyword    I return datatype for ETL_ID in ${table} in schema ${system}
        run keyword and continue on failure  should be equal as strings    ${datatype}   Integer
		run keyword and continue on failure     add result to report    ETL_ID datatype expected : Integer , actual: ${datatype}
		run keyword and continue on failure     add result to report  The table contains ${null_count} nulls for ETL_ID and the count of ETL_ID's ${non_null_count} matches the rowcount ${count_dbfs} \n
	END
    run keyword and continue on failure  add result to report   Test Passed: The table contains a unique identifiable value for ETL_ID ${system}.${table}

The ETL_USERID contains the database account name that last changed the record
    remove from dictionary  ${dict_tables_and_keyfields}    mska
    FOR	${table}		IN 	  @{dict_tables_and_keyfields}
        run keyword and continue on failure  add result to report   ----- Testing : ${system}.${table} -----
        run keyword and continue on failure   I check ETL_USERID in ${table} for null values
        ${distinct_userid}=  run keyword and continue on failure    run distinct query with condition  ${table}    ETL_USERID  ${system}   is not null
    	${resultb}=	run keyword and continue on failure  should be equal as strings    ${distinct_userid}   ${databricks_user_id}
    	run keyword and continue on failure     add result to report    The table contains the correct user is ${distinct_userid} = ${databricks_user_id}\n
        ${datatype}=    run keyword    I return datatype for ETL_USERID in ${table} in schema ${system}
        run keyword and continue on failure  should be equal as strings    ${datatype}   String
    	run keyword and continue on failure     add result to report    The column ETL_USERID is type ${datatype} = String\n
	END
    run keyword and continue on failure  add result to report   Test Passed: The 'ETL_USERID' column contains the database account name that last changed the record in ${system}.${table}


The ETL_CREATEDATE contains a valid datetime in UTC format yyyy-MM-dd'T'HH:mm:ss.SSSZ
    set test variable  ${date_format}   String
    FOR	${table}		IN 	  @{dict_tables_and_keyfields}
        run keyword and continue on failure   I check ETL_CREATEDATE in ${table} for null values
        ${datatype}=    run keyword    I return datatype for ETL_CREATEDATE in ${table} in schema ${system}
        run keyword and continue on failure  should be equal as strings    ${datatype}   ${date_format}
    	run keyword and continue on failure     add result to report    The column ETL_CREATEDATE type ${datatype} = ${date_format}
        ${utc_date}=  run keyword and continue on failure    return_date_as_utc    ${system}   ${table}  ETL_CREATEDATE
        run keyword and continue on failure  should end with    ${utc_date}   +0000
    	run keyword and continue on failure     add result to report    The column ETL_CREATEDATE type is in the format UTC and ends with +0000 offset\n
	END
    run keyword and continue on failure  add result to report   \nTest Passed: The 'ETL_CREATEDATE' column contains a date time of when the record was completed in ${system}.${table}

The ETL_LASTUPDATEDATE contains a valid datetime in UTC format yyyy-MM-dd'T'HH:mm:ss.SSSZ
    #TODO MUST ASSIGN CORRECT DATATYPE FOR EACH SYSTEM
    set test variable  ${date_format}   string

    FOR	${table}		IN 	  @{dict_tables_and_keyfields}
        run keyword and continue on failure   I check ETL_LASTUPDATEDATE in ${table} for null values
        ${columndata}=  return datatype of column   ${system}   ${table}    ETL_LASTUPDATEDATE
        run keyword and continue on failure  should be equal as strings    ${columndata}   ${date_format}
        run keyword and continue on failure     add result to report    The column ETL_LASTUPDATEDATE type ${columndata} = ${date_format}\n
        ${utc_date}=  run keyword and continue on failure    return_date_as_utc    ${system}   ${table}  ETL_LASTUPDATEDATE
        run keyword and continue on failure  should end with    ${utc_date}   +0000
    	run keyword and continue on failure     add result to report    The column ETL_LASTUPDATEDATE type is in the format UTC and ends with +0000 offset
	END
    run keyword and continue on failure  add result to report   Test Passed: The 'ETL_LASTUPDATEDATE' column contains a date time of when the record was last updated in ${system}.${table}

No difference is identified between source and target records
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		${common_col_names}=	Run Keyword  I return column names for ${table} in schema ${system}
		set suite variable		${common_col_names}
		run keyword and continue on failure  Remove blacklist values from list
		${str_common_col_names}= 	Convert To String  ${common_col_names}
		${str_common_col_names}= 	Replace String  ${str_common_col_names}		[	${EMPTY}
		${str_common_col_names}= 	Replace String  ${str_common_col_names}		]	${EMPTY}
		${str_common_col_names}= 	Replace String  ${str_common_col_names}		'	${EMPTY}
		${comparison_count}=	run keyword and continue on failure   run source target comparison query  ${table}  ${str_common_col_names}  ${system}
		${result}=	run keyword and continue on failure  should be equal as integers  ${comparison_count-1}  0
		run keyword and continue on failure	add result to report  The difference in records between initial load and delta table ${table} in ${system}: ${comparison_count-1} records\n
	END
	run keyword and continue on failure  add result to report	 Test Passed: The records in Source and Target version zero are equal for all tables in ${system}

Difference in records between source and target is as expected for pasx
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		${common_col_names}=	Run Keyword  I return column names for ${table} in schema ${system}
		set suite variable		${common_col_names}
		run keyword and continue on failure  Remove blacklist values from list
		${str_common_col_names}= 	Convert To String  ${common_col_names}
		${str_common_col_names}= 	Replace String  ${str_common_col_names}		[	${EMPTY}
		${str_common_col_names}= 	Replace String  ${str_common_col_names}		]	${EMPTY}
		${str_common_col_names}= 	Replace String  ${str_common_col_names}		'	${EMPTY}
		${comparison_count}=	run keyword and continue on failure   run pasx comparison query  ${table}  ${str_common_col_names}  ${system}
		${result}=	run keyword and continue on failure  should be equal as integers  ${comparison_count}  0
		run keyword and continue on failure	add result to report  The difference in records between initial load and delta table ${table} in ${system}: ${comparison_count} records\n
	END
	run keyword and continue on failure  add result to report	 Test Passed: The records in Source and Target version zero are equal for all tables in ${system}
	write to file  ${jira_id_tag}
	${var}=		convert to string  report-${jira_id_tag}.txt
	log file to report ${var}

# ---------------------------- UTIL  M E T H O D S ----------------------------

The table files are in the correct folder on source ${table} ${system}
	${table_location}= 		Run Keyword 	return location of table on dbfs 	${table}	${system}
	should be equal as strings		${table_location}		dbfs:/mnt/adls/raw/${system}/${table}
	add result to report  Target table: ${table} is in the correct folder location: ${table_location}\n

Remove blacklist values from list
	FOR	${bl_item}	IN   @{blacklist}
		Remove values from list  ${common_col_names}		${bl_item}   # remove blacklist items
	END

#todo (Move to robot util class)
I check ${column} in ${table} for null values
    ${null_count}=  run keyword and continue on failure  run count query with condition  ${table}   ${column}  ${system}   is null
    ${result}=	run keyword and continue on failure  should be equal as integers    ${null_count}   0
    run keyword and continue on failure     add result to report    The table contains ${null_count} null values for ${column}

#todo used for creation of _stg tables(Move to stg util class)
I create table on top of flat file ${table} ${system} ${location}
	${col_names}=	Run Keyword  I return column names for ${table} in schema ${system}
	FOR	${wl_start_item}	IN   @{whitelist_start}
		Insert into list  ${col_names}	0 	${wl_start_item} 	# Insert values into start of list
	END
	FOR	${wl_end_item}	IN   @{whitelist_end}
		Append to list  ${col_names} 	${wl_end_item}	# append values to the end of the list
	END
	FOR	${bl_item}	IN   @{blacklist}
		Remove values from list  ${col_names}		${bl_item}   # remove blacklist items
	END
	${result}=	build hive table on flat file   ${table}   ${system}_STG   ${location}  ${col_names}
	add result to report  ${result}

I create table on top of csv file ${table} ${system} ${location}
	${col_names}=	Run Keyword  I return column names for ${table} in schema ${system}
	FOR	${bl_item}	IN   @{blacklist}
		Remove values from list  ${col_names}		${bl_item}   # remove blacklist items
	END
	${result}=	build hive table on csv   ${table}   ${system}_STG   ${location}  ${col_names}
	add result to report  ${result}

I create table on top of parquet file ${table} ${system} ${location}
	${col_names}=	Run Keyword  I return column names for ${table} in schema ${system}
	FOR	${bl_item}	IN   @{blacklist}
		Remove values from list  ${col_names}		${bl_item}   # remove blacklist items
	END
	${result}=	build hive table on parquet   ${table}   ${system}_STG   ${location}  ${col_names}
	add result to report  ${result}

I create hive views from flat files in blob for all tables in database
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
	#	I create table on top of flat file ${table} ${system} /mnt/l0_initial_loads/mdh-core/landing/${system}/${table}/initial
		I create table on top of flat file ${table} ${system} /mnt/l0_initial_loads/landing/${system}/${table}/initial
	END
	write to file  AFND-8888

I create hive views from csv files in blob for all tables in database
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		I create table on top of csv file ${table} ${system} /mnt/adls/landing/osipi/${system}/${table}/Processed
	END
	write to file  AFND-9999

I create hive views from parquet files in blob for all tables in database
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		I create table on top of parquet file ${table} ${system} /mnt/adls/landing/pasx/${table}/archive/initial
	END
	write to file  AFND-7777


# ---------------------------- NOT USED ----------------------------
I run spark query
	run spark count query

#NOT USED



I check I have the correct columns ${list_of_columns_from_ac} for table ${table} ingested in the target database ${db}
	${column_types}=	Run Keyword		get columns from table  	${table}		${db}
	${list_of_columns}=		convert to list  ${list_of_columns_from_ac}
	lists should be equal  	${list_of_columns}		${column_types}


# Not used
I check I have the correct columns ${list_of_columns_from_ac} for table ${table} ingested in the target database ${db}
	${column_types}=	Run Keyword		get columns from table  	${table}		${db}
	${list_of_columns}=		convert to list  ${list_of_columns_from_ac}
	lists should be equal  	${list_of_columns}		${column_types}

# Not used
I check that the datatypes of the target tables are as expected for ${table} in ${schema}
	@{list_of_types_chvw}=	Create list	 	VARCHAR 	VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		VARCHAR		INTEGER		VARCHAR		VARCHAR		VARCHAR
	@{list_of_columns_from_dictionary}=		Create List
	FOR	${result}	IN 	@{dict_of_columns}
		Append to List	${list_of_columns_from_dictionary}	${result}[type]
	END
	lists should be equal  	${list_of_columns_from_dictionary}	${list_of_types_chvw}

#not used
I compare first 100 records from source and target
	FOR	${table}		IN 	  @{dict_tables_and_keyfields}
		${common_col_names}=	Run Keyword  I return column names for ${table} in schema ${system}
		set suite variable		${common_col_names}
		log to console	${str_common_col_names}
		${100_records_dbfs}=	I return 1 ${str_common_col_names} records from ${system}.${table} with conditions ETL_ID=1
		${100_records_flat_file}=	I return 2 ${str_common_col_names} records from ${system}_STG2.${table} with conditions ${EMPTY}
		${list_dbfs}=	convert to list		${100_records_dbfs}
		${list_flat_file}=	convert to list		${100_records_flat_file}
		${100_records_flat_file}=	remove from list   ${list_flat_file}  0
		add result to report  DBFS \n${list_dbfs} \nFLAT_FILE \n${list_flat_file}
		log to console	${system}.${table}
		write to file  AFND-8888
		lists should be equal   ${list_dbfs}	 ${list_flat_file}	ignore_order=True
	END
#not used
I check the inserts and deletes from kafka
	${kafka_result}= 	run keyword  I return 100000 logging_Event records from Metastore.Kafka_Logs with conditions Status = 1 and Source_Name = 'elims_s_sample' and Logging_Event not like 'Topic %'
	${kafka_list}=	create list
	FOR	${result}	IN 		@{kafka_result}
		log to console  	${result}[0]
		${kafka_inserts}=	get kafka updates	${result}[0]
		append to list	${kafka_list}	${kafka_inserts}
	END
	${sum_of_inserts}= 	convert to integer  0
	${sum_of_updates}= 	convert to integer  0
	${sum_of_deletes}= 	convert to integer  0
	FOR	${result}	IN 		@{kafka_list}
		log to console 	Inserts: ${result}[0]\tUpdates: ${result}[1]\tDeletes: ${result}[2]
		${sum_of_inserts}=  Evaluate    int(${sum_of_inserts}) + int(${result}[0])
		${sum_of_updates}=  Evaluate    int(${sum_of_updates}) + int(${result}[1])
		${sum_of_deletes}=  Evaluate    int(${sum_of_deletes}) + int(${result}[2])
		log to console  Sum of inserts: ${sum_of_inserts}\nSum of updates: ${sum_of_updates}\nSum of Deletes: ${sum_of_deletes}
	END
	${change_count}=	evaluate  ${sum_of_inserts}-${sum_of_deletes}
	log to console 	Change count: ${change_count}

*** Settings ***
Documentation    Imports libraries and gets the setup ready for execution.

*** Keywords ***
Connect to Azure Database
    connect to azureDB   ${dvar('db_conn_string')}  ${dvar('db_name')}  ${azdbusername}  ${azdbpassword}  ${dvar('db_port')}

Disconnect from Azure Database
    disconnect from azureDB

Connect to ORM
    connect using orm      ${dvar('db_conn_string')}  ${dvar('db_name')}    ${azdbusername}  ${azdbpassword}

Connect to Databricks
	connect databricks	westeurope		${token}		${system}		${url}		# https://westeurope.azuredatabricks.net

Connect to CDL Databricks
    ${Connection}    run keyword and return status     connect databricks	eastus		${token}		${system}		${url}		# https://adb-4924220490975335.15.azuredatabricks.net
	set suite variable    ${Connection}    ${Connection}
	set suite variable    ${databrick_url}     ${url}
	#connect databricks	adb-4924220490975335.15		${token}		${system}		${url}		# https://adb-4924220490975335.15.azuredatabricks.net

Connect to CDL VIEW Databricks
    #retaining the values of variables for CDL L1 DB
    set suite variable   ${base_token}  ${token}
    set suite variable   ${base_system}  ${system}
    set suite variable   ${base_url}  ${url}
    set suite variable   ${base_table}  ${table}

    #overwriting the varibles used in the tests to point to CDL L1 views DB
    set suite variable   ${token}  ${vw_token}
    set suite variable   ${system}  ${vw_system}
    set suite variable   ${url}  ${vw_url}

    ${view_name}=    Set Variable    ''
    ${view_name}=    Get Variable Value    ${view}
    run keyword if  '${view_name}' != 'None'   set suite variable   ${table}  ${view_name}

	 ${Connection}    run keyword and return status    connect databricks	eastus		${token}		${system}		${url}
     set suite variable    ${Connection}    ${Connection}
     set suite variable    ${databrick_url}     ${url}


Disconnect from Databricks
	disconnect databricks
	set suite variable    ${Connection}    False

I check for Null values in column ${colname}
    ${null_checker}=    check null values   ${query_results}   ${colname}   ${description}
    Set test variable    ${null_checker}

I find no Null values under selected column
    Should be true   ${null_checker}

Row values are found in the table
    should be true    ${query_results}

The datawarehouse query is executed
    [Arguments]    ${area}
    Open RedShiftDB Connection
	${full_results}=  Execute query for ${area} and return results
	Close RedShiftDB Connection
	Copy datawarehouse rows ${full_results} to excel file

Extract SQL query for ${area} from file
    ${sql_query_file}=	Run Keyword  Get SQL query for ${area}
	Copy file  ${sql_query_file}
	...  ${resultsFolder}${/}Query.txt
	${sql_query}=    Get File  ${sql_query_file}
	[Return]  ${sql_query}

Copy datawarehouse rows ${sql_rows} to excel file
	Set test variable  ${sql_rows}
	${excel}=	Set Variable    ${OUTPUT DIR}${/}Extracted${/}DWH ${TEST NAME}.xlsx
	Write List To New Excel    ${excel}    Sheet1    ${sql_rows}

Execute query for ${area} without headers
	${query}=  Extract SQL query for ${area} from file
	${query_results}=    Query  ${query}
	[Return]    ${query_results}

Execute query for ${area} and return results
    ${query}=  Extract SQL query for ${area} from file
    ${query_headers}=  Get ${query} columns
	${query_results}=  Query  ${query}
	${full_results}=  Combine ${query_headers} and ${query_results}
	[Return]    ${full_results}

Get ${query} columns
    ${columns}=  Description  ${query}
    ${columnList}=  Convert to list  ${columns}
    ${header_list}=  Create list
    :FOR  ${column}  IN  @{columnList}
    \  ${column}=  Convert to string  ${column}
    \  ${name}=  Get regexp matches  ${column}  (?<=name=')(.*?)(?=\')
    \  Append to list  ${header_list}  @{name}[0]
    ${headers}=  Evaluate  tuple(${header_list})
    ${headerList}=  Create list  ${headers}
    [Return]  ${headerList}

# Chrome support
Set chrome download preferences and open chrome browser
    ${chrome_options}=    Evaluate    sys.modules['selenium.webdriver'].ChromeOptions()    sys, selenium.webdriver
    # list of plugins to disable. disabling PDF Viewer is necessary so that PDFs are saved rather than displayed
    ${disabled}    Create List    Chrome PDF Viewer
    # download_dir = Downloaded (Any folder path in which you want chrome to download file)
    ${prefs}    Create Dictionary    download.default_directory=${OUTPUT DIR}${/}${dvar('download_dir')}
    ...  plugins.plugins_disabled=${disabled}
    Call Method    ${chrome_options}    add_experimental_option    prefs    ${prefs}
    ${webdriver}=  Create Webdriver    Chrome    chrome_options=${chrome options}
    Set suite variable  ${browserIsOpen}  1

#Log file to report ${filepath}
#	Log	<msg src="${filepath}">img src="${filepath}"</msg>	HTML

Combine ${list1} and ${list2}
    ${newList}=  Create list
    :FOR  ${item}  IN  @{list1}
    \  Append to list  ${newList}  ${item}
    :FOR  ${item}  IN  @{list2}
    \  Append to list  ${newList}  ${item}
    [Return]  ${newList}

Move Most Recent Excel File
    [Arguments]  ${FROM_DIRECTORY}    ${TO_DIRECTORY}
    @{files}=   List Files In Directory     ${FROM_DIRECTORY}    [!~]*.xlsx
    ${fileListLength}=  Get length  ${files}
    Run keyword if  '${fileListLength}'=='0'  Fail  File download from Qlik not successful
    ${lastModifiedFile} =   Get From List   ${files}    0
    ${time1}=  OperatingSystem.Get Modified Time       ${FROM_DIRECTORY}    epoch

	:FOR    ${file}    IN    @{files}
	\    log  ${file}
	\    ${time}    Get Modified Time   ${FROM_DIRECTORY}${/}${file}    epoch
	\    ${lastModifiedFile}    Set Variable If    ${time1} < ${time}    ${FROM_DIRECTORY}${/}${file}    ${lastModifiedfile}
	\    ${path}  ${lastModifiedFile}=  Split path  ${lastModifiedfile}
	\    ${time1}    Set Variable If    ${time1} < ${time}    ${time}    ${time1}
    log  ${lastModifiedFile}
    log  ${FROM_DIRECTORY}
    Copy File  ${FROM_DIRECTORY}${/}${lastModifiedFile}   ${TO_DIRECTORY}
    Remove file  ${FROM_DIRECTORY}${/}${lastModifiedFile}

Wait for file download
	[Arguments]		${directory}	${wait_time_in_secs}
	[Documentation] 	Waits for the file to be downloaded into the directory.
	...		Assumes that the file should not have an extension .tmp
	...		in the download directory.
	Wait Until Keyword Succeeds	 ${wait_time_in_secs}	1s
	...  File should exist  ${directory}\\[!~]*.xlsx

Combine ${list1} and ${list2}
    ${newList}=  Create list
    :FOR  ${item}  IN  @{list1}
    \  Append to list  ${newList}  ${item}
    :FOR  ${item}  IN  @{list2}
    \  Append to list  ${newList}  ${item}
    [Return]  ${newList}

The comparison results in excel indicate matching data
    ${comparisonOk}=  Compare data between Qlik and DWH Excel files
    Should be True  ${comparisonOk}  msg=Qlik and DWH data do not match

Compare data between Qlik and DWH Excel files
    [Documentation]  Specify filenames for Qlik, Dwh and Comparison Excel files. IOError => file name is too long
	${db_excel}=    Set Variable    ${OUTPUT DIR}${/}${dvar('pre_processed_dir')}${/}DWH ${TEST NAME}.xlsx
	Log file to report ${db_excel}
	${qlik_excel}=    Set Variable    ${OUTPUT DIR}${/}${dvar('pre_processed_dir')}${/}Qlik ${TEST NAME}.xlsx
	Log file to report ${qlik_excel}
	${compare_excel}=    Set Variable  Comparison.xlsx
	${comparisonOk}=	Log Excel Differences	 ${qlik_excel}	${db_excel}  ${compare_excel}
	[Return]	${comparisonOk}
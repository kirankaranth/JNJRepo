*** Settings ***
Documentation    Imports libraries and gets the setup ready for execution.
Library           Selenium2Library    WITH NAME        SelLib
Library           Screenshot
Library           RequestsLibrary
Library           json
Library           Collections
Library           DateTime
Library           OperatingSystem
Library           String
Resource          Sql-alchemy/Support-SQL-Alchemy.robot
Resource          Databricks/Support-Databricks.robot
Resource          DataAnalytics/Support_DataAnalytics.robot
Variables  		  Data/Web/Dev/variable.py
*** Variables ***
${OUTPUT_DIR}        Output
${REPORT_DIR}     Tests/Reports
*** Keywords ***

Log file to report ${filepath}
	Log	<msg src="${filepath}">img src="${filepath}"</msg>	HTML

Add file to result
   ${filepath}=  Catenate    SEPARATOR=  report-   ${report}   .txt
   Log	<msg src="${filepath}">img src="${filepath}"</msg>	HTML

log details to report
   ${time_info}=   run keyword and continue on failure  time taken
   ${tester_name}=   get tester name
   Set test variable    ${time_info}      ${time_info}
   Set test variable    ${tester_name}    ${tester_name}
   ${temp}    run keyword and return status    Should Be Equal      ${databricks_env}    DEV
   run keyword if  ${temp}==True     test detail for dev
   run keyword if  ${temp}==False    test details for qa
   run keyword and continue on failure  add result to report       ${test_details}

test detail for dev
     ${test_details}=  run keyword and continue on failure  tabulate string   Test ran by: ${tester_name} \nTest executed on cluster url: ${url} \nTest executed in environment: ${databricks_env} \n${time_info}\nExecuted on ${execution_time}
     Set test variable     ${test_details}     ${test_details}

test details for qa
     ${test_details}=  run keyword and continue on failure  tabulate string   Test ran by: ${tester_name} \nTest executed on cluster url: ${url} \nTest executed in environment: ${databricks_env} \n${time_info}\nRelease version: ${release}\nExecuted on ${execution_time}
      Set test variable     ${test_details}     ${test_details}

log test to report
    write to file	 ${test_id_tag}
	${var}=		convert to string  report- ${test_id_tag}.txt
	log file to report ${var}

log test to report per step
    [Arguments]    ${var}
     run keyword   log details to report
     ${var}=    Convert To String     ${var}
     ${var}=    Remove String    ${var}    {    '    }    ${SPACE}
     write to file	 ${test_id_tag}-${var}
     set test variable    ${report}     ${test_id_tag}-${var}
	 ${var}=		convert to string  report-${test_id_tag}-${var}.txt
	 Copy Files    ${OUTPUT_DIR}/${var}    ${REPORT_DIR}/${jira_id_tag}/
	 Add file to result

Set JIRA tag
    ${jira_temp_tag}=   get tag from test name  ${TEST NAME}
    set test variable   ${test_id_tag}  ${jira_temp_tag}
    ${jira_temp_tag}=   Fetch From Left     ${jira_temp_tag}   _
    set test variable   ${jira_id_tag}  ${jira_temp_tag}

Setup Test
    ${execution_time_temp}=   run keyword and continue on failure     get current date
    set test variable   ${execution_time}   ${execution_time_temp}
    set jira tag
    start timer

Teardown Test
    stop timer
    log test to report per step     ${jira_id_tag}
   # run keyword   log details to report
    #run keyword   log test to report
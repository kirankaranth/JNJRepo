*** Settings ***
Documentation    Executes web based tests.

*** Keywords ***
Set chrome download preferences and open chrome browser
    ${chrome_options}=    Evaluate    sys.modules['selenium.webdriver'].ChromeOptions()    sys, selenium.webdriver
    # list of plugins to disable. disabling PDF Viewer is necessary so that PDFs are saved rather than displayed
    ${disabled}    Create List    Chrome PDF Viewer
    ${prefs}    Create Dictionary    download.default_directory=${OUTPUT DIR}${/}${wvar('download_dir')}
    ...  plugins.plugins_disabled=${disabled}
    Call Method    ${chrome_options}    add_experimental_option    prefs    ${prefs}
    ${webdriver}=  SelLib.Create Webdriver    Chrome    chrome_options=${chrome options}
    Set suite variable  ${browserIsOpen}  1

Log file to report ${filepath}
	Log	<msg src="${filepath}">img src="${filepath}"</msg>	HTML

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

# Selenium Check functionality
Check ${property} ${value} contains strings from ${stringList}
    ${complete} =   SelLib.get_text    ${property}=${value}
    Check part strings ${stringList} are displayed in ${complete}

Check part strings ${subStringList} are displayed in ${string}
    :FOR    ${text}    IN    @{subStringList}
    \   should contain      ${string}       ${text}

Check ${property} ${value} has value ${expected}
    ${actual} =     SelLib.get_text    ${property}=${value}
    should be equal as strings     ${actual}       ${expected}

Check ${property} ${value} is present with no value
    ${actual} =     SelLib.get_text    ${property}=${value}
    should be empty  ${actual}

Check ${property} ${value} has ${occurs} occurences
    ${actual} =   get_matching_xpath_count  ${property}=${value}
    Should Not Be Equal As Integers     ${occurs}       ${actual}

Check ${property} ${value} has more than ${occurs} occurence
    ${actual} =  get_matching_xpath_count  ${property}=${value}
    Should Be True     ${actual} > ${occurs}

# Click functionality
Click ${web_element} if present
    ${present}=  Run Keyword And Return Status    SelLib.Element Should Be Visible   ${web_element}
    Run Keyword If  ${present}      SelLib.click element   ${web_element}

# Wait functionality
Wait Then Click Element
    [Documentation]    Wait until page contains supplied element and then Click the element.
    [Arguments]  ${element}
    SelLib.Wait Until Page Contains Element  ${element}
    SelLib.Click Element  ${element}

Wait Until Visible Then Click Element
    [Documentation]    Wait until element is visible and then Click the element.
    [Arguments]  ${element}
    SelLib.Wait Until Element is Visible  ${element}
    SelLib.Click Element  ${element}

Wait Until Visible Then Click Element With Retry
    [Documentation]    Wait until element is visible and then Click the element. Retry if fails.
    [Arguments]  ${element}
    Wait Until Keyword Succeeds  3x  1 sec  Wait Until Element is Visible  ${element}
    SelLib.Click Element  ${element}

Wait Until Visible With Retry
    [Documentation]    Wait until element is visible. Retry if fails.
    [Arguments]  ${element}
    Wait Until Keyword Succeeds  3x  1 sec  Wait Until Element is Visible  ${element}

Wait Until Visible Then Get Element Text
    [Documentation]    Wait until element is visible and then get the text of the element.
    [Arguments]  ${element}
    Wait Until Element is Visible  ${element}
    ${ret}=  SelLib.Get Text  ${element}
    [Return]  ${ret}

Wait Then Get Element Text
    [Documentation]    Wait until page contains the supplied element and then get the text of
    ...    the element.
    [Arguments]  ${element}
    Wait Until Page Contains Element  ${element}
    ${ret}=  SelLib.Get Text  ${element}
    [Return]  ${ret}

Wait Then Assert Element Text
    [Documentation]    Wait until page contains the supplied element and make sure the text is the
    ...    same as that supplied.
    [Arguments]  ${element}  ${text}
    SelLib.Wait Until Page Contains Element  ${element}
    SelLib.Element Text Should Be  ${element}  ${text}

Wait Then Assert Element Contains Text
    [Documentation]    Wait until page contains the supplied element and make sure the text of the element
    ...    contains supplied text.
    [Arguments]  ${element}  ${text}
    SelLib.Wait Until Page Contains Element  ${element}
    SelLib.Element Should Contain Text  ${element}  ${text}

Wait Until Visible Then Assert Element Contains Text
    [Documentation]    Wait until page contains the supplied element and make sure the text of the element
    ...    contains supplied text.
    [Arguments]  ${element}  ${text}
    SelLib.Wait Until Element is Visible  ${element}
    SelLib.Element Should Contain Text  ${element}  ${text}

Wait Then Assert Stripped Text
    [Documentation]    Wait until page contains the supplied element and make sure the text is the
    ...    same as that supplied after both texts are stripped of special characters
    ...    (incl. spaces, html, new lines, carriage returns).
    [Arguments]  ${element}  ${text}
    SelLib.Wait Until Page Contains Element  ${element}
    ${element_text}=  SelLib.Get Text  ${element}
    Compare Stripped Strings  ${element_text}  ${text}

Wait Then Assert Stripped Text With Retry
    [Documentation]    Wait until page contains the supplied element (retry if it doesn't) and make sure the text is the
    ...    same as that supplied after both texts are stripped of special characters
    ...    (incl. spaces, html, new lines, carriage returns).
    [Arguments]  ${element}  ${text}
    # Wait Until Page Contains Element  ${element}
    Wait Until Keyword Succeeds  3x  1 sec  Wait Until Page Contains Element  ${element}
    ${element_text}=  SelLib.Get Text  ${element}
    Compare Stripped Strings  ${element_text}  ${text}

Wait Until Enabled
    [Documentation]    Wait until page contains the supplied element and make sure it is enabled.
    [Arguments]  ${element}
    SelLib.Wait Until Page Contains Element   ${element}
    SelLib.Element Should Be Enabled  ${element}

Wait Until Enabled Then Assert Element Text
    [Documentation]    Wait until element is enabled and make sure the text is the
    ...    same as that supplied.
    [Arguments]  ${element}  ${text}
    SelLib.Wait Until Page Contains Element   ${element}
    SelLib.Element Should Be Enabled  ${element}
    SelLib.Element Text Should Be  ${element}  ${text}

Run Keyword And Warn On Failure
    [Documentation]  Logs a warning when the given keyword fails. Does not fail the keyword.
    [Arguments]  ${keyword}  @{arguments}
    ${result}  ${output}=  Run Keyword And Ignore Error  ${keyword}  @{arguments}
    # ${result}  ${output}=  Run Keyword And Continue On Failure  ${keyword}  @{arguments}
    Run Keyword If  '${result}'!='PASS'
    ...  Run Keywords  Log  ${\n}Warning: Keyword '${keyword} @{arguments}' failed.  WARN
    ...    AND  SelLib.Capture Page Screenshot
    [Return]  ${output}

Element Should Be Visible and Enabled
    [Documentation]  Checks element is enabled and visible.
    [Arguments]  ${element}
    # Element Should Be Visible  ${element}
    SelLib.Wait Until Element is Visible  ${element}
    SelLib.Element Should Be Enabled  ${element}

*** Settings ***
Documentation     Testing functionality through Robot Framework
Suite Setup       Setup highest level suite
Test Timeout
Force Tags        MVA
Resource          Support.robot

*** Keywords ***
Setup highest level suite
    ${d.test_server}=    Set Variable    ${test_server}
    #Reset screenshot count variable

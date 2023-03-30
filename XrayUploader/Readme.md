XRay Uploader Workflow
======================

The details on XRay Uploader are in confluence at: [XRay Overview](https://confluence.jnj.com/display/ABET/XRay+Overview)

# Dependencies

1. Install Python.
2. Install dependencies on test machine: pip install -r Test_Management_Automation/XRayUploader/requirements.txt
   -  This may require you to install Visual Studio Tools v14.0 installed on a Windows system.
3. Make sure you have the required access on the JIRA to create tests in your project area.

# Create tests under Jira Stories
This workflow explains how tests are created in Jira before adding results for them in XRay test execution.
- The process here is that when the story is created high level acceptance criterion are identified for the story.
- Tests are identified which cover these acceptance criterion.
- Tests outline in gherkin format are added in Robot Scripts and also second level keywords are created.
- The 2nd level keyword contain variable names for which the values may be substituted in actual test execution.
- Ensure the documentation of these tests contain the text Jira-ID: <Story Jira ID>. This is used to link the test to the jira story.

## Execute
- Execute the tests to upload with dryrun flag. E.g.
```
python driver.py -i <story tag> --dryrun
```
- At this point the tests are not actually executed and dryrun creates a report outline enough for us to create tests in XRay.
- Use the command below to upload the results into XRay.

```
python Test_Management_Automation\XRayUploader\xray_uploader.py -r <Absolute path to Output.xml report> -jprj <Project Code>  -u <username> -p <password> -jurl <Jira Url> -a Create_Test
```

An example could be:
```
python Test_Management_Automation\XRayUploader\xray_uploader.py -r C:/Tests/Output/Output.xml -jprj AABD  -u <username> -p <password> -jurl "https://dev.jira.jnj.com" -a Create_Test
```

## Review tests
These are manual steps to be performed for the test to be reviewed.
- Tests are then be assigned to the executing user.
- Tests are updated to start progress.
- Test Affected and Fixed versions are added as required.
- Test are sent for review marking the status as Done.
- Test are assigned to TQ for review.
- TQ reviews the tests ensuring they cover the Acceptance criterion and mark them as Complete.

At this point the tests are ready for execution.

## Test implementation
At the same time that the tests are under review the tester can continue implementing the steps
under step 2 of keywords to add logic to implement tests and add actual data value in the Data
folder section.


# Create test result under XRay test execution
Once the implementation of tests is completed the tests are executed again without the dryrun flag i.e.
```
python driver.py -i <story tag>
```

## Upload test result into XRay
- If uploading tests to an existing test execution then that value is passed.
- If value is not passed for test execution then a new test execution is created.
- If an existing test execution is used and the test result already exists in the test execution the result is overwritten.
- Upload using command:

```
python Test_Management_Automation\XRayUploader\xray_uploader.py -r <Absolute path to Output.xml> -jprj <Project Jira Code>  -u <username> -p <password> -jurl <Jira Url optional> -a Add_Result -xtenv <Test Environments> -xex <Jira Test Execution ID: Optional>
```
E.g.
```
python Test_Management_Automation\XRayUploader\xray_uploader.py -r "Z:\Output\Output.xml" -jprj AAOU  -u <username> -p <password> -jurl "https://dev.jira.jnj.com" -a Add_Result -xtenv Web ios -xex AABD-124
```

## Review test Execution
- Once the test results are uploaded to the test execution the Test execution status is changed to in-review and assigned to TQ.
- TQ reviews the test execution and once the approved the test execution is marked as Completed.

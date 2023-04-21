#!/bin/groovy
@Library('jpm_shared_lib@1.x') _
import org.jnj.*
def args = [:]
args.debug = true
args.manifestSourcesFile = 'manifest-sources.yaml'


new pipelines.stdPipeline().execute(args)
/*
ciuser = 'JBGA-CIUser-Service-Account'
def commitAuthor = 'Unknown'
def branchName = ''
def pytestOpts = "-x -s --disable-pytest-warnings"
def pytestHtmlOpts = "--html=report.html --self-contained-html"

node {
    cleanWs()

    checkout scm
    println "Current Branch: " + env.BRANCH
    branchName = env.BRANCH
    //environment = env.ENVIRONMENT
    //branchName = "main"
    environment = "dev"
    script {
        commitAuthor = sh(returnStdout: true, script: 'git log --format="%ae" | head -1')
        println "Last commit by: " + commitAuthor
    }
}



bitbucketStatusNotify(buildState: 'INPROGRESS')

echo "********************** CD **************************"
echo "Deploy $branchName at " + new Date()

if(environment == "dev" || environment == "pqa" || environment == "qa" || environment == "prod") {



    databricks_url = getDatabxUrl(environment)
    databricks_token = getDatabxToken(environment)
    prophecy_root_folder = "./src/"
    try{

                stage('Update jenkinsProfile'){
                        jenkinsProfile.addImageSources([
                            desc: 'jbga-docker private docker registry',
                            type: 'privateRegistry',
                            url: 'https://jbga-docker.artifactrepo.jnj.com',  // team's private docker registry
                            credsId: 'jnj-eat-artifactory'
                        ])
                    }

                stage('Build Prophecy Project') {
                    prophecyBuild(databricks_token, databricks_url, prophecy_root_folder)
                }
                stage('Test Prophecy Project') {
                    prophecyTest(databricks_token, databricks_url, prophecy_root_folder)
                }
                if (env.BRANCH_NAME ==~ /(development)/) {
                    stage('Deploy Prophecy Project') {  
                        prophecyDeploy(databricks_token, databricks_url, prophecy_root_folder)
                    }
                }

                currentBuild.result = "SUCCESS"

                bitbucketStatusNotify(buildState: 'SUCCESSFUL')
    }catch (Exception e) {
                currentBuild.result = "FAILED"
                bitbucketStatusNotify(buildState: 'FAILED')
    }
    


}
else {
    echo "Deployment skipped for branch $branchName, environment $environment"
    currentBuild.result = "SUCCESS"

    bitbucketStatusNotify(buildState: 'SUCCESSFUL')     
}


*/


/**
 * Build the project
 */
def prophecyBuild(token, url, folder) {


        

        ensure.insideDockerContainer('jbga-docker.artifactrepo.jnj.com/cdl-prophecy-deploy') {
            checkout scm

            withCredentials([string(credentialsId: token, variable: 'deToken')]) {
                sh """
                    echo "[DEFAULT]\nhost = $url\ntoken = $deToken" > '/home/databricks/.databrickscfg'
                    export DATABRICKS_HOST=$url
                    export DATABRICKS_TOKEN=$deToken
                """
            }

                sh """
                    export LC_ALL=en_US.UTF-8
                    pbt build --path  $folder
                """

        }

}

/**
 * Test the project
 */
def prophecyTest(token, url, folder) {
    ensure.insideDockerContainer('jbga-docker.artifactrepo.jnj.com/cdl-prophecy-deploy') {
        checkout scm

            sh """
                export LC_ALL=en_US.UTF-8
                pbt test --path  $folder
            """
    }
}


/**
 * Test the project
 */
def prophecyDeploy(token, url, folder) {
    ensure.insideDockerContainer('jbga-docker.artifactrepo.jnj.com/cdl-prophecy-deploy') {
        checkout scm

        withCredentials([string(credentialsId: token, variable: 'deToken')]) {
            sh """
                echo "[DEFAULT]\nhost = $url\ntoken = $deToken" > '/home/databricks/.databrickscfg'
                export DATABRICKS_HOST=$url
                export DATABRICKS_TOKEN=$deToken 
                export LC_ALL=en_US.UTF-8              
                pbt deploy --path  $folder
            """
        }


    }
}


/**
 * The Jenkins secret text
 * @param environment
 * @return
 */
def getDatabxToken(environment) {
    if(environment == "dev" || environment == "pqa" )
        return 'databx-cdl-dev-access-token'
    else if(environment == "qa")
        return 'databx-cdl-qa-access-token'
    else if(environment == "prod")
        return 'databx-cdl-prd-access-token'
}

/**
 * Get the databricks URL based on branch. Default (feature/bugfix) is DEV
 * @param environment
 * @return
 */
static def getDatabxUrl(environment) {
    if(environment == "dev" || environment == "pqa" )
        return 'https://adb-8388065977403376.16.azuredatabricks.net'
    else if(environment == "qa")
        return 'https://adb-8388065977403376.16.azuredatabricks.net'
    else if(environment == "prod")
        return 'https://adb-8388065977403376.16.azuredatabricks.net'
}

/**
 * Get the databricks folder based on environment.
 * @param environment
 * @return DBRX path
 */
static def getDeployPath(environment) {
    if(environment == "dev")
        return '/Shared/CICD/l1_curation/development'
    else if(environment == "pqa")
        return '/Shared/CICD/l1_curation/pqa'
    else if(environment == "qa")
        return '/Shared/CICD/l1_curation/qa'
    else if(environment == "prod")
        return '/Shared/CICD/l1_curation/prod'
}
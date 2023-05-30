#!/bin/groovy



def run(jobManifest) {
  def jobVars = jobManifest.getJobVars()

   //ciuser = 'JEKT-CIUser-Service-Account'
    def commitAuthor = jobVars.commitAuthor
    def branchName = jobVars.projectBranch
    def environment = "dev"
    def appName = "md_l1"
    environment = jobVars.jpmEnvironment.toLowerCase() 
    def version = jobVars.calculatedVersion

    
  echo "********************** CD $environment **************************"
  echo "Deploy $branchName at $environment" + new Date()

    //def ver =  version.getVersionDataFromManifest(jobVars)

    databricks_url = getDatabxUrl(environment)
    databricks_token = getDatabxToken(environment)
    prophecy_root_folder = "./src/"

    stage('Update jenkinsProfile'){
            jenkinsProfile.addImageSources([
                desc: 'jekt-docker private docker registry',
                type: 'privateRegistry',
                url: 'https://jekt-docker.artifactrepo.jnj.com',  // team's private docker registry
                credsId: 'jnj-eat-artifactory'
            ])
        }
    stage('Deploy Prophecy Project') {
        prophecyDeploy(databricks_token, databricks_url, prophecy_root_folder, version, environment, appName)
    }

}

def expectsManifest() {
  return true
}





/**
 * Deploy the project
 */
def prophecyDeploy(token, url, folder, version, environment, appName) {
    ensure.insideDockerContainer('jekt-docker.artifactrepo.jnj.com/cdl-prophecy-deploy:1.1.2') {
        checkout scm

        withCredentials([string(credentialsId: token, variable: 'deToken')]) {
            sh """
                echo "[DEFAULT]\nhost = $url\ntoken = $deToken" > '/home/databricks/.databrickscfg'
                export DATABRICKS_HOST=$url
                export DATABRICKS_TOKEN=$deToken 
                export LC_ALL=en_US.UTF-8 
                python updateDeploy.py $environment $appName            
                pbt deploy --release-version $version --project-id 97 --path  $folder
                python updatePermissions.py $environment $appName      
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
    if(environment == "qa" || environment == "QA")
        return 'databx-cdl-l1-qa-access-token'
    else if(environment == "prod" || environment == "PROD")
        return 'databx-cdl-l1-prd-access-token'
    else
        return 'databx-cdl-l1-dev-access-token'
}

/**
 * Get the databricks URL based on branch. Default (feature/bugfix) is DEV
 * @param environment
 * @return
 */
static def getDatabxUrl(environment) {
    if(environment == "qa" || environment == "QA")
        return "https://adb-3309966811984132.12.azuredatabricks.net"
    else if(environment == "prod" || environment == "PROD")
        return "https://adb-7108733885606347.7.azuredatabricks.net"
    else
        return 'https://adb-4924220490975335.15.azuredatabricks.net'
}

return this;
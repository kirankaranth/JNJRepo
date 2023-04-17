#!/bin/groovy
def run(jobManifest) {
   def jobVars = jobManifest.getJobVars()
    def commitAuthor = jobVars.commitAuthor
    def branchName = jobVars.projectBranch
    def environment = "dev"
    environment = jobVars.jpmEnvironment.toLowerCase() 

    echo "********************** PBT Validate **************************"
    echo "Validate $branchName at " + new Date()


    prophecy_root_folder = "./src/"

                stage('Update jenkinsProfile'){
                        jenkinsProfile.addImageSources([
                            desc: 'jekt-docker private docker registry',
                            type: 'privateRegistry',
                            url: 'https://jekt-docker.artifactrepo.jnj.com',  // team's private docker registry
                            credsId: 'jnj-eat-artifactory'
                        ])
                    }
                stage('Validate Prophecy Project') {
                    prophecyValidate(prophecy_root_folder)
                }

}

def expectsManifest() {
  return true
}
/**
 * Build the project
 */
def prophecyValidate(folder) {


        ensure.insideDockerContainer('jekt-docker.artifactrepo.jnj.com/cdl-prophecy-deploy:1.0.4.2b') {
            checkout scm

                sh """
                    export LC_ALL=en_US.UTF-8
                    pbt validate --path  $folder
                """

        }

}
return this


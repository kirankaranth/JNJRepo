#!/bin/groovy



def run(jobManifest) {
    def jobVars = jobManifest.getJobVars()
    def commitAuthor = jobVars.commitAuthor
    def branchName = jobVars.projectBranch
    def environment = "dev"
    environment = jobVars.jpmEnvironment.toLowerCase() 


echo "********************** PBT Test **************************"
echo "PBT Test $branchName at " + new Date()




    prophecy_root_folder = "./src/"

                stage('Update jenkinsProfile'){
                        jenkinsProfile.addImageSources([
                            desc: 'jekt-docker private docker registry',
                            type: 'privateRegistry',
                            url: 'https://jekt-docker.artifactrepo.jnj.com',  // team's private docker registry
                            credsId: 'jnj-eat-artifactory'
                        ])
                    }
                stage('Test Prophecy Project') {
                    prophecyTest(prophecy_root_folder)
                }


}


def expectsManifest() {
  return true
}

/**
 * Test the project
 */
def prophecyTest(folder) {
    ensure.insideDockerContainer('jekt-docker.artifactrepo.jnj.com/cdl-prophecy-deploy:1.0.4.2b') {
        checkout scm

            sh """
                export LC_ALL=en_US.UTF-8
                pbt test --path  $folder
            """
    }
}

return this






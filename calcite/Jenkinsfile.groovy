pipeline {
   agent any
   parameters {
     string(
       description: "Branch to build",
       name: "branch",
       defaultValue: "master"
     )
   }
   stages {
    stage('source') {
         steps {
           cleanWs()
           dir("calcite") {
             checkout([
               $class: 'GitSCM',
               branches: [[name: "${params.branch}"]],
               doGenerateSubmoduleConfigurations: false,
               extensions: [],
               submoduleCfg: [],
               userRemoteConfigs: [[
                 url: 'https://github.com/datametica1/calcite.git'
               ]]
             ])
           }
           dir("init-gradle") {
             checkout([
               $class: 'GitSCM',
               branches: [[name: "calcite-test"]],
               doGenerateSubmoduleConfigurations: false,
               extensions: [[
                 $class: 'SparseCheckoutPaths',
                 sparseCheckoutPaths:
                 [[
                   path: 'calcite/*'
                 ]]
               ]],
               submoduleCfg: [],
               userRemoteConfigs: [[
                 credentialsId: 'build.manager',
                 url: 'http://taiga.datametica.com/migrations1/mig-v2.git'
              ]]
            ])
          }
        }
      }
      stage('update release version') {
         steps {
            sh '''
              #!/bin/bash
              cd calcite
              set -x
              release_number=$(curl -s -i -H "Accept: application/json" -u admin:admin123 "http://nexus2.datametica.com:8081/nexus/service/local/artifact/maven/redirect?r=thirdparty&g=org.apache.calcite&a=calcite-core&v=RELEASE" | grep Location | rev | cut -d/ -f2 | rev)
              released_major_ver=$(echo $release_number | cut -d. -f1,2)
              released_minor_ver=$(echo $release_number | cut -d. -f3)

              #get the calcite version from gradle.properties
              repo_calcite_ver=$(grep -e ^calcite.version gradle.properties | cut -d= -f2)
              repo_calcite_major_ver=$(echo $repo_calcite_ver | cut -d. -f1,2)
              repo_calcite_minor_ver=$(echo $repo_calcite_ver | cut -d. -f3)

              if [[ $(echo "$released_major_ver > $repo_calcite_major_ver" | bc -l) ]]; then
                  new_release_ver=$(expr $released_minor_ver + 1)
                  new_release_ver=$released_major_ver.$new_release_ver
                  sed -i "s/$repo_calcite_ver/$new_release_ver/" gradle.properties
              fi
              cd -
              ls init-gradle
              cd init-gradle/calcite
              ls
              cp calcite-x.xx.x.pom calcite-$new_release_ver.pom
              cp calcite-core-x.xx.x.pom calcite-core-$new_release_ver.pom
              sed -i "s/x.xx.x/$new_release_ver/g" calcite*-$new_release_ver.pom
              cd -

            '''
         }
      }
      stage('build') {
         steps {
           sh '''
             #!/bin/bash
             cd calcite      
             ./gradlew clean build -x test
             cd -
           '''
         }
      }
      stage('publish') {
          steps {
           sh '''
             #!/bin/bash
             nexus_repo='http://nexus2.datametica.com:8081/nexus/content/repositories/thirdparty'
             group='org/apache/calcite'
             cd calcite
             ./gradlew --init-script ../init-gradle/calcite/init.gradle.kts publishAllPublicationsToSecretNexusRepository --no-daemon
             cd ../init-gradle/calcite/
             echo "Uploading the pom files"
             for pom in $(ls *.pom | grep -v x.xx.x)
             do
               if [[ $pom == *"calcite-core"* ]]; then
                 version=$(echo $pom | cut -d '-' -f 3 | cut -d '.' -f 1,2,3)
                 curl -v -u admin:admin123 --upload-file $pom $nexus_repo/$group/calcite-core/$version/$pom
               else
                 version=$(echo $pom | cut -d '-' -f 2 | cut -d '.' -f 1,2,3)
                 curl -v -u admin:admin123 --upload-file $pom $nexus_repo/$group/calcite/$version/$pom
               fi
             done
             cd ..
           '''
          }
      }
   }
   post {
   // Clean after build
     always {
       cleanWs(cleanWhenNotBuilt: false,
                deleteDirs: true,
                disableDeferredWipeout: true,
                notFailBuild: true,
                patterns: [[pattern: '.gitignore', type: 'INCLUDE'],
                [pattern: '.propsfile', type: 'EXCLUDE']])
     }
   }
}


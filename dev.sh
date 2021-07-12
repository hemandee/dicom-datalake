#! /bin/bash
set -e

#!/bin/bash
# Set some default values:

export SAM_CLI_TELEMETRY=0
DIRECTORY=$(cd `dirname $0` && pwd)

usage()
{
  echo "Usage: dev.sh [ -i | install ] 
                         [ -d | deploy ]
                         [ -s | setup ]
                         [ -t | test [S3ObjectName] ]
                         
        Example: 
            ./dev.sh install                 
            ./dev.sh -t bucketname sample.dcm"
  exit 2
}


function print(){
  printf "======= $1 ======\n"
}
function INSTALL (){
  INSTALLED_COMPONENTS=["SAM-CLI"]
  print "INSTALL PHASE INITIATED $INSTALLED_COMPONENTS"
  if ! command -v sam &> /dev/null
   then
     print "INSTALLING SAM-CLI FOR PYTHON 3"
     print "pip3 install aws-sam-cli"
     pip3 install aws-sam-cli
    
   else
    print "SAM CLI FOUND SKIP INSTALL"
  fi
  if ! command -v aws &> /dev/null
   then
     print "INSTALLING awscli FOR PYTHON 3"
     print "pip3 install awscli"
     pip3 install awscli
   else
    print "AWS CLI FOUND SKIP INSTALL"
  fi
   

  # print "INSTALLING PYTHON PACKAGES LOCALLY"
  # pip3 install -r  $DIRECTORY/dicomparser_function/requirements.txt -t $DIRECTORY/dicomparser_function/dicomparser/lib/
  if ! command -v aws &> /dev/null
   then
     print "INSTALLING python-lambda-local FOR PYTHON 3"
     print "pip3 install python-lambda-local "
     pip3 install python-lambda-local 
   else
    print "python-lambda-local FOUND SKIP INSTALL"
  fi
  pip3 install -r  $DIRECTORY/dicomparser_function/requirements.txt -t $DIRECTORY/lib/
  print "FINISHED INSTALL PHASE"
  exit
}

 function SETUP () {
  print "LAMBDA INITIAL DEPLOYMENT"
  print "SAM DEPLOY"
  printf "\n Keep Default Values for Stack Name,  SAM Configuration file, SAM configuration environment \n Press Enter to Continue"
  read 
  sam deploy --guided --stack-name dicomDev --capabilities CAPABILITY_IAM --config-env development
  exit
}

function DEPLOY () {
cd $DIRECTORY
sam build
# To keep unzipped deployment under 250MB (HARD LIMIT), remove known included AWS Lambda Packages
KNOWN_PACKAGES="boto3 botocore"
EXTRA_KNOWN_PACKAGES="six jmespath s3transfer"
for DIR_NAME in $KNOWN_PACKAGES
do
  print "Removing Duplicate Lambda Library $DIRECTORY/.aws-sam/build/DicomFunction/$DIR_NAME"
  rm -rf $DIRECTORY/.aws-sam/build/DicomFunction/$DIR_NAME
done
sam deploy --config-env development ##--template template.yaml
exit
}

function TEST () {
configLocation=$DIRECTORY/samconfig.toml
region=$(awk -v FS="region =" 'NF>1{print $2}' $configLocation | tr -d \")
stackName=$(awk -v FS="stack_name =" 'NF>1{print $2}' $configLocation | tr -d \")
dicomBucket=$(aws --region $region cloudformation describe-stacks --stack-name $stackName --query 'Stacks[0].Outputs[?OutputKey==`DicomBucket`].OutputValue' --output text)
print "DICOM BUCKET $dicomBucket ||||  Object: $1"
cd $DIRECTORY/dicomparser_function/
event=$(sam local generate-event s3 put --bucket $dicomBucket --region $region --key $1)
echo $event > event.json
# python-lambda-local -t 60 -f lambda_handler -l ../lib dicomparser_function/dicomparser/app.py dicomparser_function/event.json
python-lambda-local -t 60 -f lambda_handler -l ../lib dicomparser/app.py event.json
# rm event.json
exit
}

function LAMBDA_TEST () {
configLocation=$DIRECTORY/samconfig.toml
region=$(awk -v FS="region =" 'NF>1{print $2}' $configLocation | tr -d \")
stackName=$(awk -v FS="stack_name =" 'NF>1{print $2}' $configLocation | tr -d \")
dicomBucket=$(aws --region $region cloudformation describe-stacks --stack-name $stackName --query 'Stacks[0].Outputs[?OutputKey==`DicomBucket`].OutputValue' --output text)
print "DICOM BUCKET $dicomBucket ||||  Object: $1"
sam build
print "Hit Enter to Continue"
read 
sam local generate-event s3 put --bucket $dicomBucket --region $region --key $1 | sam local invoke --env-vars env.json -d 5890 -e  - DicomFunction

exit
}


if [ -z "$1" ]
then
  usage
fi 
while [ "$1" != "" ]; do
    case $1 in
         install | -i )         
          INSTALL
          ;;
        deploy | -d )         
          DEPLOY 
          ;;
        setup | -s )         
          SETUP
           ;;
        test | -t )         
          TEST $2
           ;;  
        lambda-local )
          LAMBDA_TEST $2    
          ;;                                               
        * | '' | h | --help | "\n" )
           usage
           exit 1
    esac
    shift
done






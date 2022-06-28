#!/usr/bin/bash

if [[ $# -eq 0 ]] ; then
    echo "Enter the bucket name as ./teardownn_aws.sh your-bucket"
    exit 0
fi

AWS_ID=$(aws sts get-caller-identity ---query Account --output text | cat)

echo "Reading infrastructure variables from .env"
source .env

echo "Reading state value  from state.log"
source state.log

echo "Deleting bucket "$1" and its contents"
aws s3 rm s3://$1 -- recursive --output text >> teardownn.log
aws s3api delete-bucket --bucket $1 --output text >> teardownn.log

echo "Terminating  EC2 instance"
aws ec2  terminate-instance --instance-ids  $EC2_ID --region $AWS_REGION >> teardownn.log

MY_IP=$(curl -s http://whatismyip.akamai.com/)

echo "Delete EC2 security group ingress"
aws ec2 revoke-security-group-ingress --group-id $EC2_SECURITY_GROUP_ID --protocol tcp --port 22 --cidr $MY_IP/24 --output text >> teardownn.log

echo " Delete EC2 security group egress"
aws ec2 revoke-security-group-egress --group-id $EC2_SECURITY_GROUP_ID --protocol tcp --port 8080 --cidr $MY_IP/32 --output text >> teardownn.log

echo "Terminating EMR  Cluster "$SERVICE_NAME""
EMR_CLUSTER_ID=$(aws emr list-clusters --active --query 'Clusters[?Name==`'$SERVICE_NAME'`].Id' --output text)
aws emr terminate-clusters --cluster-ids $EMR_CLUSTER_ID >> teardownn.log

echo "Deleting EC2 security group"
sleep 60
aws ec2 delete-security-group --group-id $EC2_SECURITY_GROUP_ID --output text >> teardownn.log

echo " Terminating Redshift cluster "$SERVICE_NAME"" 
aws redshift delete-cluster --skip-final-cluster-snapshot --cluster-identifier $SERVEICE_NAME --output text >> teardownn.log

echo "Dissociating AmazonS3ReadOnlyAccess Policy from "$IAM_ROLE_NAME" role"
aws iam detach-role-policy --role-name $EC2_IAM_ROLE --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess --output text >> teardownn.log

echo "Dissociating AWSGlueConsoleFullAccess Policy from "$IAM_ROLE_NAME" role"
aws iam detach-role-policy --role-name $IAM_ROLE_NAME --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess --output text >> teardownn.log

echo "Deleting role "$IAM_ROLE_NAME""
aws iam delete-role --role-name $IAM_ROLE_NAME --output text >> teardownn.log

EC2_IAM_ROLE=sde-ec2-s3-emr-rs-access
echo "Remove role from instance profile"
aws iam remove-role-from-instance-profile --instance-profile-name $EC2_IAM_ROLE-instance-profile --output text >> teardownn.log

echo "Deleting role instance profile "$EC2_IAM_ROLE"-instance-profile"
aws iam delete-instance-profile --instance-profile-name $EC2_IAM_ROLE-instance-profile --output text >> teardownn.log

echo "Dissociating AmazonS3FullAccess Policy from "$IAM_ROLE_NAME" role"
aws iam detach-role-policy --role-name $EC2_IAM_ROLE --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess --output text >> teardownn.log

echo "Dissociating AmazonEMRFullAccess_v2 Policy from "$IAM_ROLE_NAME" role"
aws iam detach-role-policy --role-name $EC2_IAM_ROLE --policy-arn arn:aws:iam::aws:policy/AmazonEMRFullAccess_v2 --output text >> teardownn.log

echo "Dissociating AmazonRedshiftAllCommandsFullAccessPolicy Policy from "$IAM_ROLE_NAME" role"
aws iam detach-role-policy --role-name $EC2_IAM_ROLE --policy-arn arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccessPolicy --output text >> teardownn.log

echo "Deleting SSH Key"
aws ec2 delete-key-pair --key-name sde-key --region $AWS_REGION >> setup.log
rm -f sde-key.pem

rm -f teardownn.log setup.log trust-policy.json
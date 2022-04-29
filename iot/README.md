
# IoT 集成说明

## 创建IoT Thing
```
AWS_REGION=us-east-1
THING_NAME=test_thing_1

THING_ARN=$(aws iot create-thing --thing-name $THING_NAME --region $AWS_REGION|jq -r ".thingArn")

aws iot create-keys-and-certificate --set-as-active --region $AWS_REGION \
  --public-key-outfile $THING_NAME.public.key \
  --private-key-outfile $THING_NAME.private.key \
  --certificate-pem-outfile $THING_NAME.certificate.pem > create_cert_and_keys_response

CERTIFICATE_ARN=$(jq -r ".certificateArn" create_cert_and_keys_response)
echo $CERTIFICATE_ARN


POLICY_NAME=${THING_NAME}_policy
aws iot create-policy --policy-name ${POLICY_NAME} --region $AWS_REGION  \
  --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action": ["iot:Publish","iot:Receive","iot:Subscribe","iot:Connect"],"Resource":"*"}]}'

aws iot attach-policy --policy-name $POLICY_NAME \
  --target $CERTIFICATE_ARN --region $AWS_REGION 

aws iot attach-thing-principal --thing-name $THING_NAME \
  --principal $CERTIFICATE_ARN --region $AWS_REGION 

curl -O https://www.amazontrust.com/repository/AmazonRootCA1.pem

aws iot describe-thing --thing-name $THING_NAME --region $AWS_REGION 
```

## 发送消息
JAVA Sample Code见IotBasicPub.java

## 接收消息
JAVA Sample Code见IotBasicSub.java

## Fleet Provisioning by Claim
JAVA Sample Code见FleetProvisioningSample.java



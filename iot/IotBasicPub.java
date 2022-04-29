package com.awstest.aws.iot;

//https://github.com/aws/aws-iot-device-sdk-java-v2/blob/main/samples/BasicPubSub/src/main/java/pubsub/PubSub.java

import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.mqtt.MqttClientConnection;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.crt.mqtt.MqttMessage;
import software.amazon.awssdk.crt.mqtt.QualityOfService;
import software.amazon.awssdk.iot.AwsIotMqttConnectionBuilder;
import java.util.concurrent.CompletableFuture;

public class IotBasicPub {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			String clientId = "test1";
			String region = "us-east-1";
			String topic = "iot/test";
			String message = "MarkTest1";
			String certPath = "/Users/markxue/temp/iot/test1/test_thing_1.certificate.pem";
			String keyPath = "/Users/markxue/temp/iot/test1/test_thing_1.private.key";
			String rootCaPath = "/Users/markxue/temp/iot/test1/AmazonRootCA1.pem";
			String endpoint = "aa9k7247xcy11-ats.iot.us-east-1.amazonaws.com"; // aws iot describe-endpoint --endpoint-type iot:Data-ATS  --region us-east-1
			int port = 8883;
			
			
	        MqttClientConnectionEvents callbacks = new MqttClientConnectionEvents() {
	            @Override
	            public void onConnectionInterrupted(int errorCode) {
	                if (errorCode != 0) {
	                    System.out.println("Connection interrupted: " + errorCode + ": " + CRT.awsErrorString(errorCode));
	                }
	            }
	
	            @Override
	            public void onConnectionResumed(boolean sessionPresent) {
	                System.out.println("Connection resumed: " + (sessionPresent ? "existing session" : "clean session"));
	            }
	        };
	        
	        AwsIotMqttConnectionBuilder builder = AwsIotMqttConnectionBuilder.newMtlsBuilderFromPath(certPath, keyPath);
	        
	        builder.withCertificateAuthorityFromPath(null, rootCaPath);
	        
            builder.withConnectionEventCallbacks(callbacks)
	            .withClientId(clientId)
	            .withWebsockets(false)
                .withWebsocketSigningRegion(region)
	            .withEndpoint(endpoint)
	            .withPort((short)port)
	            .withCleanSession(true)
	            .withProtocolOperationTimeoutMs(60000);
	        
            MqttClientConnection connection = builder.build();
            
            CompletableFuture<Boolean> connected = connection.connect();
            boolean sessionPresent = connected.get();
            System.out.println("Connected to " + (!sessionPresent ? "new" : "existing") + " session!");

            CompletableFuture<Integer> published = connection.publish(new MqttMessage(topic, message.getBytes(), QualityOfService.AT_LEAST_ONCE, false));
            System.out.println(published.get());
            
            
            CompletableFuture<Void> disconnected = connection.disconnect();
            disconnected.get();
            
            System.out.println("Complete!");
            
		} catch (Exception e) {
			e.printStackTrace();
		}
        
	}

}

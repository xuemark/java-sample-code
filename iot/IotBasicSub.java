
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.mqtt.MqttClientConnection;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.crt.mqtt.QualityOfService;
import software.amazon.awssdk.iot.AwsIotMqttConnectionBuilder;

public class IotBasicSub {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			String clientId = "test1";
			String region = "us-east-1";
			String topic = "iot/test";
			String message = "test_message";
			String certPath = "xxx.certificate.pem";
			String keyPath = "xxx.private.key";
			String rootCaPath = "AmazonRootCA1.pem";
			String endpoint = "xxx-ats.iot.us-east-1.amazonaws.com"; // aws iot describe-endpoint --endpoint-type iot:Data-ATS  --region us-east-1
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

            CountDownLatch countDownLatch = new CountDownLatch(2); // Exit in case of receive N messages 
            
            CompletableFuture<Integer> subscribed = connection.subscribe(topic, QualityOfService.AT_LEAST_ONCE, (receivedMessage) -> {
                String payload = new String(receivedMessage.getPayload(), StandardCharsets.UTF_8);
                System.out.println("MESSAGE: " + payload);
                countDownLatch.countDown();
            });

            System.out.println(subscribed.get());

            countDownLatch.await();
            
            CompletableFuture<Void> disconnected = connection.disconnect();
            disconnected.get();
            
            System.out.println("Complete!");
            
		} catch (Exception e) {
			e.printStackTrace();
		}  
	}
}

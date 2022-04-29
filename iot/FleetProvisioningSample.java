
import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.CrtResource;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.crt.Log;
import software.amazon.awssdk.crt.mqtt.MqttClientConnection;
import software.amazon.awssdk.crt.mqtt.MqttClientConnectionEvents;
import software.amazon.awssdk.crt.mqtt.QualityOfService;
import software.amazon.awssdk.iot.AwsIotMqttConnectionBuilder;
import software.amazon.awssdk.iot.iotidentity.IotIdentityClient;
import software.amazon.awssdk.iot.iotidentity.model.CreateCertificateFromCsrRequest;
import software.amazon.awssdk.iot.iotidentity.model.CreateCertificateFromCsrResponse;
import software.amazon.awssdk.iot.iotidentity.model.CreateCertificateFromCsrSubscriptionRequest;
import software.amazon.awssdk.iot.iotidentity.model.CreateKeysAndCertificateRequest;
import software.amazon.awssdk.iot.iotidentity.model.CreateKeysAndCertificateResponse;
import software.amazon.awssdk.iot.iotidentity.model.CreateKeysAndCertificateSubscriptionRequest;
import software.amazon.awssdk.iot.iotidentity.model.ErrorResponse;
import software.amazon.awssdk.iot.iotidentity.model.RegisterThingRequest;
import software.amazon.awssdk.iot.iotidentity.model.RegisterThingResponse;
import software.amazon.awssdk.iot.iotidentity.model.RegisterThingSubscriptionRequest;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.HashMap;
import com.google.gson.Gson;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class FleetProvisioningSample {

	public static void main(String[] args) {
        
        try {

			String clientId = "test1";
			String region = "us-east-1";
			String topic = "iot/test";
			String message = "test_message";
			String certPath = "xxx.certificate.pem";
			String keyPath = "xxx.private.key";
			String rootCaPath = "AmazonRootCA1.pem";
			String endpoint = "xxx-ats.iot.us-east-1.amazonaws.com"; // aws iot describe-endpoint --endpoint-type iot:Data-ATS  --region us-east-1
			String templateName = "test";
			String templateParameters = "{"
					+ "    \"SerialNumber\": \"123\", "
					+ "    \"AWS::IoT::Certificate::Id\": \"xxx\""
					+ "}";
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
                    	
            IotIdentityClient iotIdentityClient = new IotIdentityClient(connection);

            CompletableFuture<Boolean> connected = connection.connect();
            try {
                boolean sessionPresent = connected.get();
                System.out.println("Connected to " + (!sessionPresent ? "new" : "existing") + " session!");
            } catch (Exception ex) {
                throw new RuntimeException("Exception occurred during connect", ex);
            }

            try {
                CreateKeysAndCertificateSubscriptionRequest createKeysAndCertificateSubscriptionRequest = new CreateKeysAndCertificateSubscriptionRequest();
                CompletableFuture<Integer> keysSubscribedAccepted = iotIdentityClient.SubscribeToCreateKeysAndCertificateAccepted(
                        createKeysAndCertificateSubscriptionRequest,
                        QualityOfService.AT_LEAST_ONCE,
                        FleetProvisioningSample::onCreateKeysAndCertificateAccepted);

                keysSubscribedAccepted.get();
                System.out.println("Subscribed to CreateKeysAndCertificateAccepted");

                CompletableFuture<Integer> keysSubscribedRejected = iotIdentityClient.SubscribeToCreateKeysAndCertificateRejected(
                        createKeysAndCertificateSubscriptionRequest,
                        QualityOfService.AT_LEAST_ONCE,
                        FleetProvisioningSample::onRejectedKeys);

                keysSubscribedRejected.get();
                System.out.println("Subscribed to CreateKeysAndCertificateRejected");


                RegisterThingSubscriptionRequest registerThingSubscriptionRequest = new RegisterThingSubscriptionRequest();
                registerThingSubscriptionRequest.templateName = templateName;

                CompletableFuture<Integer> subscribedRegisterAccepted = iotIdentityClient.SubscribeToRegisterThingAccepted(
                        registerThingSubscriptionRequest,
                        QualityOfService.AT_LEAST_ONCE,
                        FleetProvisioningSample::onRegisterThingAccepted,
                        FleetProvisioningSample::onException);

                subscribedRegisterAccepted.get();
                System.out.println("Subscribed to SubscribeToRegisterThingAccepted");

                CompletableFuture<Integer> subscribedRegisterRejected = iotIdentityClient.SubscribeToRegisterThingRejected(
                        registerThingSubscriptionRequest,
                        QualityOfService.AT_LEAST_ONCE,
                        FleetProvisioningSample::onRejectedRegister,
                        FleetProvisioningSample::onException);

                subscribedRegisterRejected.get();
                System.out.println("Subscribed to SubscribeToRegisterThingRejected");

                CompletableFuture<Integer> publishKeys = iotIdentityClient.PublishCreateKeysAndCertificate(
                        new CreateKeysAndCertificateRequest(),
                        QualityOfService.AT_LEAST_ONCE);

                publishKeys.get();
                System.out.println("Published to CreateKeysAndCertificate");

                waitForKeysRequest();

                gotResponse = new CompletableFuture<>();

                System.out.println("RegisterThing now....");
                RegisterThingRequest registerThingRequest = new RegisterThingRequest();
                registerThingRequest.certificateOwnershipToken = createKeysAndCertificateResponse.certificateOwnershipToken;
                registerThingRequest.templateName = templateName;

                if (templateParameters != null && templateParameters != "") {
                    registerThingRequest.parameters = new Gson().fromJson(templateParameters, HashMap.class);
                }

                CompletableFuture<Integer> publishRegister = iotIdentityClient.PublishRegisterThing(
                        registerThingRequest,
                        QualityOfService.AT_LEAST_ONCE);

                System.out.println("####### I am here");
                try {
                    publishRegister.get();
                    System.out.println("Published to RegisterThing");
                } catch(Exception ex) {
                    throw new RuntimeException("Exception occurred during publish", ex);
                }
                gotResponse.get();
                waitForRegisterRequest();

            } catch (Exception e) {
                throw new RuntimeException("Exception occurred during connect", e);
            }

            CompletableFuture<Void> disconnected = connection.disconnect();
            disconnected.get();
        } catch (CrtRuntimeException | InterruptedException | ExecutionException ex) {
            System.out.println("Exception encountered: " + ex.toString());
        }

        CrtResource.waitForNoResources();
        System.out.println("Complete!");
        
	}
	
	static CreateKeysAndCertificateResponse createKeysAndCertificateResponse;
	static CompletableFuture<Void> gotResponse;
    static RegisterThingResponse registerThingResponse;

	
    public static void waitForKeysRequest() {
        try {
            // Wait for the response.
            int loopCount = 0;
            while (loopCount < 30 && createKeysAndCertificateResponse == null) {
                if (createKeysAndCertificateResponse != null) {
                    break;
                }
                System.out.println("Waiting...for CreateKeysAndCertificateResponse");
                loopCount += 1;
                Thread.sleep(50L);
            }
        } catch (InterruptedException e) {
            System.out.println("Exception occured");
        }
    }
    
    public static void waitForRegisterRequest() {
        try {
            // Wait for the response.
            int loopCount = 0;
            while (loopCount < 30 && registerThingResponse == null) {
                if (registerThingResponse != null) {
                    break;
                }
                System.out.println("Waiting...for registerThingResponse");
                loopCount += 1;
                Thread.sleep(50L);
            }
        } catch (InterruptedException e) {
            System.out.println("Exception occured");
        }
    }
	
    static void onCreateKeysAndCertificateAccepted(CreateKeysAndCertificateResponse response) {
        System.out.println("CreateKeysAndCertificate response certificateId: " + response.certificateId);
        if (response != null) {
            createKeysAndCertificateResponse = response;
        } else {
            System.out.println("CreateKeysAndCertificate response is null");
        }
        gotResponse.complete(null);
    }
    
    static void onRejectedKeys(ErrorResponse response) {
        System.out.println("CreateKeysAndCertificate Request rejected, errorCode: " + response.errorCode +
                ", errorMessage: " + response.errorMessage +
                ", statusCode: " + response.statusCode);

        gotResponse.complete(null);
        System.exit(1);
    }
    static void onRegisterThingAccepted(RegisterThingResponse response) {
    	System.out.println(response.toString());
        System.out.println("RegisterThing response thingName: " + response.thingName);
        if (response != null) {
            gotResponse.complete(null);
            registerThingResponse = response;
        } else {
            System.out.println("RegisterThing response is null");
        }
    }
    
    static void onRejectedRegister(ErrorResponse response) {

        System.out.println("RegisterThing Request rejected, errorCode: " + response.errorCode +
                ", errorMessage: " + response.errorMessage +
                ", statusCode: " + response.statusCode);

        gotResponse.complete(null);
        System.exit(1);
    }
    static void onException(Exception e) {
        e.printStackTrace();
        System.out.println("Exception occurred " + e);
    }

}

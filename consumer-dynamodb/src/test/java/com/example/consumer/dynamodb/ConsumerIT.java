package com.example.consumer.dynamodb;

import static org.assertj.core.api.Assertions.assertThat;

import io.floci.testcontainers.FlociContainer;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

@Testcontainers
class ConsumerIT {

    @Container
    static final FlociContainer floci = new FlociContainer();

    private static final String STREAM_NAME = "test-events";
    private static final String TABLE_NAME = "test-events";

    private KinesisClient kinesisClient;
    private DynamoDbClient dynamoDbClient;
    private Vertx vertx;

    @BeforeEach
    void setUp() {
        StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(floci.getAccessKey(), floci.getSecretKey()));
        URI endpoint = URI.create(floci.getEndpoint());
        Region region = Region.of(floci.getRegion());

        kinesisClient = KinesisClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .credentialsProvider(credentials)
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();

        dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(endpoint)
                .region(region)
                .credentialsProvider(credentials)
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();

        vertx = Vertx.vertx();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get();
        }
        if (kinesisClient != null) {
            kinesisClient.close();
        }
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
    }

    @Test
    void shouldConsumeKinesisEventsAndWriteToDynamoDB() throws Exception {
        createStream();
        publishTestEvents(3);

        JsonObject config = new JsonObject()
                .put("awsEndpointUrl", floci.getEndpoint())
                .put("awsRegion", floci.getRegion())
                .put("awsAccessKeyId", floci.getAccessKey())
                .put("awsSecretAccessKey", floci.getSecretKey())
                .put("streamName", STREAM_NAME)
                .put("tableName", TABLE_NAME);

        vertx.deployVerticle(
                        ConsumerVerticle.class.getName(),
                        new DeploymentOptions()
                                .setThreadingModel(ThreadingModel.VIRTUAL_THREAD)
                                .setConfig(config))
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        Thread.sleep(3000);

        var scanResponse =
                dynamoDbClient.scan(ScanRequest.builder().tableName(TABLE_NAME).build());
        List<Map<String, AttributeValue>> items = scanResponse.items();

        assertThat(items).hasSize(3);
        assertThat(items).allSatisfy(item -> {
            assertThat(item).containsKey("id");
            assertThat(item).containsKey("name");
            assertThat(item).containsKey("createdAt");
        });
    }

    private void createStream() throws InterruptedException {
        kinesisClient.createStream(CreateStreamRequest.builder()
                .streamName(STREAM_NAME)
                .shardCount(1)
                .build());
        for (int i = 0; i < 30; i++) {
            StreamStatus status = kinesisClient
                    .describeStream(b -> b.streamName(STREAM_NAME))
                    .streamDescription()
                    .streamStatus();
            if (StreamStatus.ACTIVE.equals(status)) {
                return;
            }
            Thread.sleep(500);
        }
    }

    private void publishTestEvents(int count) throws Exception {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper()
                .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
                .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        for (int i = 0; i < count; i++) {
            com.example.model.Event event = new com.example.model.Event(
                    java.util.UUID.randomUUID().toString(), "test-event-" + i, java.time.Instant.now());
            kinesisClient.putRecord(PutRecordRequest.builder()
                    .streamName(STREAM_NAME)
                    .partitionKey(event.id())
                    .data(SdkBytes.fromByteArray(mapper.writeValueAsBytes(event)))
                    .build());
        }
    }
}

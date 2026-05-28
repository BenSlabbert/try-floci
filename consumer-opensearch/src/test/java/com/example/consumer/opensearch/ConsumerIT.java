package com.example.consumer.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.floci.testcontainers.FlociContainer;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
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
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

@Testcontainers
class ConsumerIT {

    @Container
    static final FlociContainer floci = new FlociContainer();

    private static final String STREAM_NAME = "test-events";
    private static final String INDEX_NAME = "test-events";

    private KinesisClient kinesisClient;
    private Vertx vertx;
    private HttpServer mockOpenSearch;
    private final AtomicInteger indexedCount = new AtomicInteger(0);

    @BeforeEach
    void setUp() throws Exception {
        StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(floci.getAccessKey(), floci.getSecretKey()));

        kinesisClient = KinesisClient.builder()
                .endpointOverride(URI.create(floci.getEndpoint()))
                .region(Region.of(floci.getRegion()))
                .credentialsProvider(credentials)
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();

        vertx = Vertx.vertx();

        mockOpenSearch = vertx.createHttpServer().requestHandler(req -> {
            if ("PUT".equals(req.method().name())) {
                indexedCount.incrementAndGet();
                req.response()
                        .setStatusCode(201)
                        .putHeader("Content-Type", "application/json")
                        .end("{\"result\":\"created\"}");
            } else {
                req.response().setStatusCode(200).end("{}");
            }
        });
        mockOpenSearch.listen(0).toCompletionStage().toCompletableFuture().get();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mockOpenSearch != null) {
            mockOpenSearch.close().toCompletionStage().toCompletableFuture().get();
        }
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get();
        }
        if (kinesisClient != null) {
            kinesisClient.close();
        }
    }

    @Test
    void shouldConsumeKinesisEventsAndIndexToOpenSearch() throws Exception {
        createStream();
        publishTestEvents(3);

        String openSearchEndpoint = "http://localhost:" + mockOpenSearch.actualPort();
        JsonObject config = new JsonObject()
                .put("awsEndpointUrl", floci.getEndpoint())
                .put("awsRegion", floci.getRegion())
                .put("awsAccessKeyId", floci.getAccessKey())
                .put("awsSecretAccessKey", floci.getSecretKey())
                .put("streamName", STREAM_NAME)
                .put("opensearchEndpoint", openSearchEndpoint)
                .put("indexName", INDEX_NAME);

        vertx.deployVerticle(
                        ConsumerVerticle.class.getName(),
                        new DeploymentOptions()
                                .setThreadingModel(ThreadingModel.VIRTUAL_THREAD)
                                .setConfig(config))
                .toCompletionStage()
                .toCompletableFuture()
                .get();

        Thread.sleep(4000);

        assertThat(indexedCount.get()).isGreaterThanOrEqualTo(3);
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
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        for (int i = 0; i < count; i++) {
            Event event = new Event(UUID.randomUUID().toString(), "test-event-" + i, Instant.now());
            kinesisClient.putRecord(PutRecordRequest.builder()
                    .streamName(STREAM_NAME)
                    .partitionKey(event.id())
                    .data(SdkBytes.fromByteArray(mapper.writeValueAsBytes(event)))
                    .build());
        }
    }
}

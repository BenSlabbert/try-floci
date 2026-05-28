package com.example.consumer.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.floci.testcontainers.FlociContainer;
import io.vertx.core.Vertx;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
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

    @SuppressWarnings("resource")
    @Container
    static final GenericContainer<?> opensearch = new GenericContainer<>("opensearchproject/opensearch:2")
            .withEnv("discovery.type", "single-node")
            .withEnv("DISABLE_SECURITY_PLUGIN", "true")
            .withExposedPorts(9200)
            .waitingFor(Wait.forHttp("/_cluster/health?wait_for_status=yellow").forStatusCode(200));

    private static final String STREAM_NAME = "test-events";
    private static final String INDEX_NAME = "test-events";

    private KinesisClient kinesisClient;
    private Vertx vertx;

    @BeforeEach
    void setUp() {
        StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(floci.getAccessKey(), floci.getSecretKey()));

        kinesisClient = KinesisClient.builder()
                .endpointOverride(URI.create(floci.getEndpoint()))
                .region(Region.of(floci.getRegion()))
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
    }

    @Test
    void shouldConsumeKinesisEventsAndIndexToOpenSearch() throws Exception {
        createStream();
        publishTestEvents(3);

        String opensearchEndpoint = "http://" + opensearch.getHost() + ":" + opensearch.getMappedPort(9200);
        ConsumerVerticle verticle = new ConsumerVerticle(kinesisClient, STREAM_NAME, opensearchEndpoint, INDEX_NAME);
        vertx.deployVerticle(verticle).toCompletionStage().toCompletableFuture().get();

        Thread.sleep(4000);

        String searchUrl = opensearchEndpoint + "/" + INDEX_NAME + "/_search";
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpResponse<String> response = httpClient.send(
                HttpRequest.newBuilder().uri(URI.create(searchUrl)).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"total\"");

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        var tree = mapper.readTree(response.body());
        int total = tree.at("/hits/total/value").asInt();
        assertThat(total).isGreaterThanOrEqualTo(3);
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

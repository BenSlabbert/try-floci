package com.example.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.floci.testcontainers.FlociContainer;
import io.vertx.core.Vertx;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

@Testcontainers
class ProducerIT {

    @Container
    static final FlociContainer floci = new FlociContainer();

    private static final String STREAM_NAME = "test-events";

    private KinesisClient kinesisClient;
    private Vertx vertx;

    @BeforeEach
    void setUp() {
        kinesisClient = KinesisClient.builder()
                .endpointOverride(URI.create(floci.getEndpoint()))
                .region(Region.of(floci.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(floci.getAccessKey(), floci.getSecretKey())))
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
    void shouldPublishEventsToKinesisStream() throws Exception {
        ProducerVerticle verticle = new ProducerVerticle(kinesisClient, STREAM_NAME);
        vertx.deployVerticle(verticle).toCompletionStage().toCompletableFuture().get();

        Thread.sleep(3500);

        String shardId = kinesisClient
                .describeStream(b -> b.streamName(STREAM_NAME))
                .streamDescription()
                .shards()
                .get(0)
                .shardId();

        String shardIterator = kinesisClient
                .getShardIterator(GetShardIteratorRequest.builder()
                        .streamName(STREAM_NAME)
                        .shardId(shardId)
                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                        .build())
                .shardIterator();

        List<Event> events = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        var response = kinesisClient.getRecords(GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .limit(100)
                .build());
        for (var record : response.records()) {
            events.add(objectMapper.readValue(record.data().asByteArray(), Event.class));
        }

        assertThat(events).isNotEmpty();
        assertThat(events).allSatisfy(event -> {
            assertThat(event.id()).isNotBlank();
            assertThat(event.name()).isNotBlank();
            assertThat(event.createdAt()).isNotNull();
        });
    }
}

package com.example.producer;

import com.example.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

public class ProducerVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(ProducerVerticle.class);

    private KinesisClient kinesisClient;
    private String streamName;
    private final ObjectMapper objectMapper;
    private long timerId = -1L;

    public ProducerVerticle() {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject cfg = config();
        String endpointUrl = cfg.getString("awsEndpointUrl", "");
        String region = cfg.getString("awsRegion", "us-east-1");
        String accessKeyId = cfg.getString("awsAccessKeyId", "");
        String secretAccessKey = cfg.getString("awsSecretAccessKey", "");
        streamName = cfg.getString("streamName", "events");

        AwsCredentialsProvider credentials = buildCredentialsProvider(accessKeyId, secretAccessKey);

        var builder = KinesisClient.builder()
                .region(Region.of(region))
                .httpClientBuilder(ApacheHttpClient.builder())
                .credentialsProvider(credentials);

        if (!endpointUrl.isBlank()) {
            builder.endpointOverride(URI.create(endpointUrl));
        }

        kinesisClient = builder.build();

        vertx.executeBlocking(() -> {
                    ensureStreamExists();
                    return null;
                })
                .onSuccess(v -> {
                    timerId = vertx.setPeriodic(1000L, id -> publishEvent());
                    log.info("Producer started, publishing to stream '{}'", streamName);
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (timerId >= 0) {
            vertx.cancelTimer(timerId);
        }
        if (kinesisClient != null) {
            kinesisClient.close();
        }
        stopPromise.complete();
    }

    private static AwsCredentialsProvider buildCredentialsProvider(String accessKeyId, String secretAccessKey) {
        if (!accessKeyId.isBlank() && !secretAccessKey.isBlank()) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        }
        return DefaultCredentialsProvider.create();
    }

    private void ensureStreamExists() {
        try {
            kinesisClient.createStream(CreateStreamRequest.builder()
                    .streamName(streamName)
                    .shardCount(1)
                    .build());
            log.info("Created Kinesis stream '{}'", streamName);
        } catch (ResourceInUseException e) {
            log.info("Kinesis stream '{}' already exists", streamName);
        }

        waitForStreamActive();
    }

    private void waitForStreamActive() {
        for (int i = 0; i < 30; i++) {
            try {
                StreamStatus status = kinesisClient
                        .describeStream(b -> b.streamName(streamName))
                        .streamDescription()
                        .streamStatus();
                if (StreamStatus.ACTIVE.equals(status)) {
                    log.info("Kinesis stream '{}' is active", streamName);
                    return;
                }
                log.debug("Waiting for stream '{}' to become active (status={})", streamName, status);
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for stream", e);
            }
        }
        throw new RuntimeException("Timed out waiting for Kinesis stream '" + streamName + "' to become active");
    }

    private void publishEvent() {
        Event event = new Event(UUID.randomUUID().toString(), "event-" + System.currentTimeMillis(), Instant.now());
        vertx.<Void>executeBlocking(() -> {
                    try {
                        byte[] data = objectMapper.writeValueAsBytes(event);
                        kinesisClient.putRecord(PutRecordRequest.builder()
                                .streamName(streamName)
                                .partitionKey(event.id())
                                .data(SdkBytes.fromByteArray(data))
                                .build());
                        log.debug("Published event id={}", event.id());
                    } catch (Exception e) {
                        log.error("Failed to publish event", e);
                    }
                    return null;
                })
                .onFailure(err -> log.error("Error in publish task", err));
    }
}

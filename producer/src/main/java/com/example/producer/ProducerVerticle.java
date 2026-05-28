package com.example.producer;

import com.example.lib.KinesisVerticle;
import com.example.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

public class ProducerVerticle extends KinesisVerticle {

    private static final Logger log = LoggerFactory.getLogger(ProducerVerticle.class);

    private final ObjectMapper objectMapper;
    private long timerId = -1L;

    public ProducerVerticle() {
        this.objectMapper = createObjectMapper();
    }

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject cfg = config();
        String endpointUrl = cfg.getString("awsEndpointUrl", "");
        String region = cfg.getString("awsRegion", "us-east-1");
        String accessKeyId = cfg.getString("awsAccessKeyId", "");
        String secretAccessKey = cfg.getString("awsSecretAccessKey", "");
        streamName = cfg.getString("streamName", "events");

        var credentials = buildCredentialsProvider(accessKeyId, secretAccessKey);
        kinesisClient = buildKinesisClient(region, endpointUrl, credentials);

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

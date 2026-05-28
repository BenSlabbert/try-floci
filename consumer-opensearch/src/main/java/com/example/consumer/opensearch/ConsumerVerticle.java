package com.example.consumer.opensearch;

import com.example.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.net.URI;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

public class ConsumerVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(ConsumerVerticle.class);

    private KinesisClient kinesisClient;
    private String streamName;
    private String indexName;
    private final ObjectMapper objectMapper;

    private WebClient webClient;
    private String shardIterator;
    private String shardId;
    private long timerId = -1L;

    public ConsumerVerticle() {
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
        String opensearchEndpoint = cfg.getString("opensearchEndpoint", "http://localhost:9200");
        indexName = cfg.getString("indexName", "events");

        AwsCredentialsProvider credentials = buildCredentialsProvider(accessKeyId, secretAccessKey);

        var kinesisBuilder = KinesisClient.builder()
                .region(Region.of(region))
                .httpClientBuilder(ApacheHttpClient.builder())
                .credentialsProvider(credentials);

        if (!endpointUrl.isBlank()) {
            kinesisBuilder.endpointOverride(URI.create(endpointUrl));
        }

        kinesisClient = kinesisBuilder.build();

        URI uri = URI.create(opensearchEndpoint);
        String opensearchHost = uri.getHost();
        int opensearchPort = uri.getPort() < 0 ? 9200 : uri.getPort();

        webClient = WebClient.create(
                vertx, new WebClientOptions().setDefaultHost(opensearchHost).setDefaultPort(opensearchPort));

        vertx.executeBlocking(() -> {
                    ensureStreamExists();
                    initShardIterator();
                    return null;
                })
                .onSuccess(v -> {
                    timerId = vertx.setPeriodic(1000L, id -> pollAndIndex());
                    log.info("OpenSearch consumer started, reading from '{}', indexing to '{}'", streamName, indexName);
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (timerId >= 0) {
            vertx.cancelTimer(timerId);
        }
        if (webClient != null) {
            webClient.close();
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
        } catch (software.amazon.awssdk.services.kinesis.model.ResourceInUseException e) {
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
                    return;
                }
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for stream", e);
            }
        }
        throw new RuntimeException("Timed out waiting for stream '" + streamName + "'");
    }

    private void initShardIterator() {
        shardId = kinesisClient
                .describeStream(b -> b.streamName(streamName))
                .streamDescription()
                .shards()
                .get(0)
                .shardId();
        shardIterator = kinesisClient
                .getShardIterator(GetShardIteratorRequest.builder()
                        .streamName(streamName)
                        .shardId(shardId)
                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                        .build())
                .shardIterator();
    }

    private void refreshShardIterator() {
        shardIterator = kinesisClient
                .getShardIterator(GetShardIteratorRequest.builder()
                        .streamName(streamName)
                        .shardId(shardId)
                        .shardIteratorType(ShardIteratorType.LATEST)
                        .build())
                .shardIterator();
    }

    private void pollAndIndex() {
        vertx.<List<Event>>executeBlocking(() -> {
                    if (shardIterator == null) {
                        return List.of();
                    }
                    try {
                        var response = kinesisClient.getRecords(GetRecordsRequest.builder()
                                .shardIterator(shardIterator)
                                .limit(100)
                                .build());
                        List<Record> records = response.records();
                        shardIterator = response.nextShardIterator();
                        return records.stream()
                                .map(r -> {
                                    try {
                                        return objectMapper.readValue(r.data().asByteArray(), Event.class);
                                    } catch (Exception e) {
                                        log.error("Failed to deserialize record", e);
                                        return null;
                                    }
                                })
                                .filter(e -> e != null)
                                .toList();
                    } catch (ExpiredIteratorException e) {
                        log.warn("Shard iterator expired, refreshing");
                        refreshShardIterator();
                        return List.of();
                    } catch (Exception e) {
                        log.error("Error polling Kinesis", e);
                        return List.of();
                    }
                })
                .onSuccess(events -> {
                    if (!events.isEmpty()) {
                        log.info("Indexing {} events to OpenSearch", events.size());
                        for (Event event : events) {
                            indexEvent(event);
                        }
                    }
                })
                .onFailure(err -> log.error("Error in poll task", err));
    }

    private void indexEvent(Event event) {
        JsonObject doc = new JsonObject()
                .put("id", event.id())
                .put("name", event.name())
                .put("createdAt", event.createdAt().toString());

        webClient
                .put("/" + indexName + "/_doc/" + event.id())
                .putHeader("Content-Type", "application/json")
                .sendJsonObject(doc)
                .onSuccess(response -> {
                    if (response.statusCode() >= 200 && response.statusCode() < 300) {
                        log.debug("Indexed event id={}", event.id());
                    } else {
                        log.warn("Failed to index event id={}, status={}", event.id(), response.statusCode());
                    }
                })
                .onFailure(err -> log.error("Error indexing event id={}", event.id(), err));
    }
}

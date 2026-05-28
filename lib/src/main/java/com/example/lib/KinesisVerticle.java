package com.example.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.AbstractVerticle;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

public abstract class KinesisVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(KinesisVerticle.class);

    protected KinesisClient kinesisClient;
    protected String streamName;
    protected String shardId;
    protected String shardIterator;

    protected static ObjectMapper createObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    protected static AwsCredentialsProvider buildCredentialsProvider(String accessKeyId, String secretAccessKey) {
        if (!accessKeyId.isBlank() && !secretAccessKey.isBlank()) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        }
        return DefaultCredentialsProvider.create();
    }

    protected KinesisClient buildKinesisClient(String region, String endpointUrl, AwsCredentialsProvider credentials) {
        var builder = KinesisClient.builder()
                .region(Region.of(region))
                .httpClientBuilder(Apache5HttpClient.builder())
                .credentialsProvider(credentials);
        if (!endpointUrl.isBlank()) {
            builder.endpointOverride(URI.create(endpointUrl));
        }
        return builder.build();
    }

    protected void ensureStreamExists() {
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

    protected void waitForStreamActive() {
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

    protected void initShardIterator() {
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

    protected void refreshShardIterator() {
        shardIterator = kinesisClient
                .getShardIterator(GetShardIteratorRequest.builder()
                        .streamName(streamName)
                        .shardId(shardId)
                        .shardIteratorType(ShardIteratorType.LATEST)
                        .build())
                .shardIterator();
    }
}

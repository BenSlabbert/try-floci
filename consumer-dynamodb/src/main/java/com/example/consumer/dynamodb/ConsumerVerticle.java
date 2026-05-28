package com.example.consumer.dynamodb;

import com.example.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
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

    private final KinesisClient kinesisClient;
    private final DynamoDbClient dynamoDbClient;
    private final String streamName;
    private final String tableName;
    private final ObjectMapper objectMapper;

    private String shardIterator;
    private String shardId;
    private long timerId = -1L;

    public ConsumerVerticle(
            KinesisClient kinesisClient, DynamoDbClient dynamoDbClient, String streamName, String tableName) {
        this.kinesisClient = kinesisClient;
        this.dynamoDbClient = dynamoDbClient;
        this.streamName = streamName;
        this.tableName = tableName;
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.executeBlocking(() -> {
                    ensureStreamExists();
                    ensureTableExists();
                    initShardIterator();
                    return null;
                })
                .onSuccess(v -> {
                    timerId = vertx.setPeriodic(1000L, id -> pollAndStore());
                    log.info("DynamoDB consumer started, reading from '{}', writing to '{}'", streamName, tableName);
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        if (timerId >= 0) {
            vertx.cancelTimer(timerId);
        }
        stopPromise.complete();
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

    private void ensureTableExists() {
        try {
            dynamoDbClient.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("id")
                            .attributeType(ScalarAttributeType.S)
                            .build())
                    .keySchema(KeySchemaElement.builder()
                            .attributeName("id")
                            .keyType(KeyType.HASH)
                            .build())
                    .build());
            log.info("Created DynamoDB table '{}'", tableName);
        } catch (ResourceInUseException e) {
            log.info("DynamoDB table '{}' already exists", tableName);
        }
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

    private void pollAndStore() {
        vertx.<Void>executeBlocking(() -> {
                    if (shardIterator == null) {
                        return null;
                    }
                    try {
                        var response = kinesisClient.getRecords(GetRecordsRequest.builder()
                                .shardIterator(shardIterator)
                                .limit(100)
                                .build());
                        List<Record> records = response.records();
                        if (!records.isEmpty()) {
                            log.info("Received {} records from Kinesis", records.size());
                            for (Record record : records) {
                                storeRecord(record);
                            }
                        }
                        shardIterator = response.nextShardIterator();
                    } catch (ExpiredIteratorException e) {
                        log.warn("Shard iterator expired, refreshing");
                        refreshShardIterator();
                    } catch (Exception e) {
                        log.error("Error polling Kinesis", e);
                    }
                    return null;
                })
                .onFailure(err -> log.error("Error in poll task", err));
    }

    private void storeRecord(Record record) {
        try {
            Event event = objectMapper.readValue(record.data().asByteArray(), Event.class);
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id", AttributeValue.builder().s(event.id()).build());
            item.put("name", AttributeValue.builder().s(event.name()).build());
            item.put(
                    "createdAt",
                    AttributeValue.builder().s(event.createdAt().toString()).build());
            dynamoDbClient.putItem(
                    PutItemRequest.builder().tableName(tableName).item(item).build());
            log.debug("Stored event id={} in DynamoDB", event.id());
        } catch (Exception e) {
            log.error("Failed to store record in DynamoDB", e);
        }
    }
}

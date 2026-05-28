package com.example.consumer.dynamodb;

import com.example.lib.KinesisVerticle;
import com.example.model.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
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
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.Record;

public class ConsumerVerticle extends KinesisVerticle {

    private static final Logger log = LoggerFactory.getLogger(ConsumerVerticle.class);

    private DynamoDbClient dynamoDbClient;
    private String tableName;
    private final ObjectMapper objectMapper;

    private long timerId = -1L;

    public ConsumerVerticle() {
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
        tableName = cfg.getString("tableName", "events");

        AwsCredentialsProvider credentials = buildCredentialsProvider(accessKeyId, secretAccessKey);
        kinesisClient = buildKinesisClient(region, endpointUrl, credentials);

        var dynamoBuilder = DynamoDbClient.builder()
                .region(Region.of(region))
                .httpClientBuilder(Apache5HttpClient.builder())
                .credentialsProvider(credentials);

        if (!endpointUrl.isBlank()) {
            dynamoBuilder.endpointOverride(URI.create(endpointUrl));
        }

        dynamoDbClient = dynamoBuilder.build();

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
        if (kinesisClient != null) {
            kinesisClient.close();
        }
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
        stopPromise.complete();
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

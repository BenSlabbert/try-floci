package com.example.consumer.dynamodb;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        String endpointUrl = System.getenv("AWS_ENDPOINT_URL");
        String region = envOrDefault("AWS_REGION", "us-east-1");
        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String streamName = envOrDefault("KINESIS_STREAM_NAME", "events");
        String tableName = envOrDefault("DYNAMODB_TABLE_NAME", "events");

        StaticCredentialsProvider credentials = (accessKey != null && secretKey != null)
                ? StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
                : null;

        KinesisClientBuilder kinesisBuilder =
                KinesisClient.builder().region(Region.of(region)).httpClientBuilder(UrlConnectionHttpClient.builder());
        DynamoDbClientBuilder dynamoBuilder =
                DynamoDbClient.builder().region(Region.of(region)).httpClientBuilder(UrlConnectionHttpClient.builder());

        if (endpointUrl != null && !endpointUrl.isBlank()) {
            URI endpoint = URI.create(endpointUrl);
            kinesisBuilder.endpointOverride(endpoint);
            dynamoBuilder.endpointOverride(endpoint);
        }

        if (credentials != null) {
            kinesisBuilder.credentialsProvider(credentials);
            dynamoBuilder.credentialsProvider(credentials);
        } else {
            kinesisBuilder.credentialsProvider(DefaultCredentialsProvider.create());
            dynamoBuilder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        KinesisClient kinesisClient = kinesisBuilder.build();
        DynamoDbClient dynamoDbClient = dynamoBuilder.build();

        Vertx vertx = Vertx.vertx(new VertxOptions());
        vertx.deployVerticle(new ConsumerVerticle(kinesisClient, dynamoDbClient, streamName, tableName))
                .onSuccess(id -> log.info("DynamoDB consumer deployed: {}", id))
                .onFailure(err -> {
                    log.error("Failed to deploy DynamoDB consumer", err);
                    vertx.close();
                });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            vertx.close();
            kinesisClient.close();
            dynamoDbClient.close();
        }));
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}

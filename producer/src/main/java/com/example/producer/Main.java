package com.example.producer;

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

        KinesisClientBuilder builder =
                KinesisClient.builder().region(Region.of(region)).httpClientBuilder(UrlConnectionHttpClient.builder());

        if (endpointUrl != null && !endpointUrl.isBlank()) {
            builder.endpointOverride(URI.create(endpointUrl));
        }

        if (accessKey != null && secretKey != null) {
            builder.credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
        } else {
            builder.credentialsProvider(DefaultCredentialsProvider.create());
        }

        KinesisClient kinesisClient = builder.build();

        Vertx vertx = Vertx.vertx(new VertxOptions());
        vertx.deployVerticle(new ProducerVerticle(kinesisClient, streamName))
                .onSuccess(id -> log.info("Producer deployed: {}", id))
                .onFailure(err -> {
                    log.error("Failed to deploy producer", err);
                    vertx.close();
                });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            vertx.close();
            kinesisClient.close();
        }));
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}

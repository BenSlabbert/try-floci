package com.example.consumer.dynamodb;

import io.vertx.core.Deployable;
import io.vertx.core.ThreadingModel;
import io.vertx.core.json.JsonObject;
import io.vertx.launcher.application.HookContext;
import io.vertx.launcher.application.VertxApplication;
import io.vertx.launcher.application.VertxApplicationHooks;
import java.util.function.Supplier;

public class Main extends VertxApplication implements VertxApplicationHooks {

    public static void main(String[] args) {
        new Main(args).launch();
    }

    public Main(String[] args) {
        super(args);
    }

    @Override
    public Supplier<? extends Deployable> verticleSupplier() {
        return ConsumerVerticle::new;
    }

    @Override
    public void beforeDeployingVerticle(HookContext context) {
        context.deploymentOptions()
                .setThreadingModel(ThreadingModel.VIRTUAL_THREAD)
                .setConfig(buildConfig());
    }

    private static JsonObject buildConfig() {
        return new JsonObject()
                .put("awsEndpointUrl", getEnv("AWS_ENDPOINT_URL", ""))
                .put("awsRegion", getEnv("AWS_REGION", "us-east-1"))
                .put("awsAccessKeyId", getEnv("AWS_ACCESS_KEY_ID", ""))
                .put("awsSecretAccessKey", getEnv("AWS_SECRET_ACCESS_KEY", ""))
                .put("streamName", getEnv("KINESIS_STREAM_NAME", "events"))
                .put("tableName", getEnv("DYNAMODB_TABLE_NAME", "events"));
    }

    private static String getEnv(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}

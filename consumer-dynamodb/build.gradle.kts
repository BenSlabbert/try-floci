plugins {
    application
    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":lib"))
    implementation(libs.vertx.core)
    implementation(libs.vertx.launcher.application)
    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)

    implementation(platform(libs.aws.bom))
    implementation(libs.aws.dynamodb)
    implementation(libs.aws.apache5.client)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit5)
    testImplementation(libs.floci.testcontainers)

    testImplementation(platform(libs.aws.bom))
    testImplementation(libs.aws.kinesis)
    testImplementation(libs.aws.dynamodb)
    testImplementation(libs.aws.url.connection.client)
}

application {
    mainClass = "com.example.consumer.dynamodb.Main"
}

tasks.shadowJar {
    mergeServiceFiles()
    exclude("module-info.class")
    manifest {
        attributes(
            "Main-Class" to "com.example.consumer.dynamodb.Main",
            "Main-Verticle" to "com.example.consumer.dynamodb.ConsumerVerticle",
        )
    }
}

tasks.build {
    dependsOn(tasks.shadowJar)
}

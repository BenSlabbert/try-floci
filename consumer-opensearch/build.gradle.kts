plugins {
    application
    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":lib"))
    implementation(libs.vertx.core)
    implementation(libs.vertx.launcher.application)
    implementation(libs.vertx.web.client)
    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit5)
    testImplementation(libs.floci.testcontainers)

    testImplementation(platform(libs.aws.bom))
    testImplementation(libs.aws.kinesis)
    testImplementation(libs.aws.url.connection.client)
}

application {
    mainClass = "com.example.consumer.opensearch.Main"
}

tasks.shadowJar {
    mergeServiceFiles()
    exclude("module-info.class")
    manifest {
        attributes(
            "Main-Class" to "com.example.consumer.opensearch.Main",
            "Main-Verticle" to "com.example.consumer.opensearch.ConsumerVerticle",
        )
    }
}

tasks.build {
    dependsOn(tasks.shadowJar)
}

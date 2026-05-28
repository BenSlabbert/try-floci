plugins {
    application
    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":model"))
    implementation(libs.vertx.core)
    implementation(libs.vertx.web.client)
    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)

    implementation(platform(libs.aws.bom))
    implementation(libs.aws.kinesis)
    implementation(libs.aws.url.connection.client)

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
        attributes("Main-Class" to "com.example.consumer.opensearch.Main")
    }
}

tasks.build {
    dependsOn(tasks.shadowJar)
}

plugins {
    `java-library`
}

dependencies {
    api(project(":model"))
    api(libs.vertx.core)
    api(platform(libs.aws.bom))
    api(libs.aws.kinesis)
    implementation(libs.aws.apache.client)
    implementation(libs.slf4j.api)
    implementation(libs.logback.classic)
}

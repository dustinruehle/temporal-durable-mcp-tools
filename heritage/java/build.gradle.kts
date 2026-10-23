plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.temporal:temporal-sdk:1.30.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile> {
    options.release.set(21)
    options.encoding = "UTF-8"
}

application {
    mainClass.set("heritage.ledger.Main")
    applicationName = "heritage-java"
}

tasks.named<JavaExec>("run") {
    // Pass Ctrl-C and stdin through to the app so it can shut down cleanly.
    standardInput = System.`in`
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
    }
}

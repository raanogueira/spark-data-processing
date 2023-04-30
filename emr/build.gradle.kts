plugins {
    scala
    application
    id("com.github.johnrengelman.shadow") version "7.1.1"
}

tasks.shadowJar {
    isZip64 = true
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.scala-lang:scala-library:2.12.17")
    implementation("com.amazonaws:aws-java-sdk-emr:1.12.459")
}

application {
    mainClass.set("emr.EMRJobSubmitter")
}
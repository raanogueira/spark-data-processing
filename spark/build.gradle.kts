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
    val scala = "2.12"
    val awsSdk = "1.12.459"
    val hadoop = "3.3.5"
    val spark = "3.4.0"

    implementation("org.scala-lang:scala-library:$scala.17")
    implementation("ch.qos.logback:logback-classic:1.4.7")
    implementation("com.typesafe.scala-logging:scala-logging_$scala:3.9.5")

    implementation("org.apache.spark:spark-core_$scala:$spark")
    implementation("org.apache.spark:spark-sql_$scala:$spark")
    implementation("org.apache.hadoop:hadoop-aws:$hadoop")
    implementation("org.apache.hadoop:hadoop-common:$hadoop")

    implementation("com.amazonaws:aws-java-sdk-core:$awsSdk")
    implementation("com.amazonaws:aws-java-sdk-s3:$awsSdk")

    testImplementation("junit:junit:4.13.2")
    testImplementation("com.amazonaws:aws-java-sdk-core:1.12.459")
    testImplementation("org.scalatest:scalatest_$scala:3.2.14")
    testImplementation("org.scalatestplus:junit-4-12_$scala:3.2.2.0")
    testImplementation("org.scalacheck:scalacheck_$scala:1.17.0")
    testImplementation("io.findify:s3mock_$scala:0.2.6")
}

application {
    mainClass.set("spark.OddFinderRunner")

    applicationDefaultJvmArgs = listOf("-XX:+IgnoreUnrecognizedVMOptions",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens=java.base/java.io=ALL-UNNAMED",
            "--add-opens=java.base/java.net=ALL-UNNAMED",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
            "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED")
}
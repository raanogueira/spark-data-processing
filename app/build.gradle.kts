plugins {
    scala
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.scala-lang:scala-library:2.12.17")
    implementation("org.apache.spark:spark-core_2.12:3.4.0")
    implementation("org.apache.spark:spark-sql_2.12:3.4.0")
    implementation("com.amazonaws:aws-java-sdk-s3:1.12.316")
    implementation("org.apache.hadoop:hadoop-aws:3.3.5")
    implementation("org.apache.hadoop:hadoop-common:3.3.5")

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.scalatest:scalatest_2.12:3.2.14")
    testImplementation("org.scalatestplus:junit-4-12_2.12:3.2.2.0")
    testImplementation("org.scalacheck:scalacheck_2.12:1.17.0")
    testImplementation("io.findify:s3mock_2.12:0.2.6")

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
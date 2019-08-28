FROM maven:3-jdk-11 AS build
WORKDIR /app

# Copy pom.xml and install dependencies
COPY pom.xml ./
RUN mvn dependency:resolve

# Copy sources and generate JAR
COPY src ./src/
RUN mvn package


FROM openjdk:11-jre AS runtime
WORKDIR /app
COPY --from=build /app/target/eventhubsconsumeperf-1.0-SNAPSHOT-jar-with-dependencies.jar ./
ENTRYPOINT ["java", "-jar", "eventhubsconsumeperf-1.0-SNAPSHOT-jar-with-dependencies.jar"]

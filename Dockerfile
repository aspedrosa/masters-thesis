FROM maven:3.8-openjdk-11 AS build

WORKDIR /app

COPY src /app/src

COPY pom.xml /app

RUN mvn package

FROM openjdk:11.0-jre-slim

WORKDIR /app

COPY --from=build /app/target/orchestrator-1.0-SNAPSHOT-jar-with-dependencies.jar /app

CMD java -jar orchestrator-1.0-SNAPSHOT-jar-with-dependencies.jar Orchestrator

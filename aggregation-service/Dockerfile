# ─── Stage 1: Build ───────────────────────────────────────────────────────────
FROM maven:3.9.7-eclipse-temurin-21 AS builder

WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn package -DskipTests -q

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

RUN addgroup -S cinema && adduser -S cinema -G cinema
USER cinema

COPY --from=builder /build/target/aggregation-service-*.jar app.jar

EXPOSE 8082

ENTRYPOINT ["java", "-jar", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=75", "app.jar"]

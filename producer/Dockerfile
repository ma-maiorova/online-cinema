# ─── Stage 1: Build ───────────────────────────────────────────────────────────
FROM maven:3.9.7-eclipse-temurin-21 AS builder

WORKDIR /build

# Сначала копируем только pom.xml для кэширования зависимостей
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Копируем исходники и собираем
COPY src ./src
RUN mvn package -DskipTests -q

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Создаём непривилегированного пользователя
RUN addgroup -S cinema && adduser -S cinema -G cinema
USER cinema

COPY --from=builder /build/target/producer-*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=75", "app.jar"]

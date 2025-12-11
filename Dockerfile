# ========== Stage 1: Build the application ==========
FROM maven:3.9.6-eclipse-temurin-17 AS build

WORKDIR /app

# Copy pom.xml and download dependencies first (cache optimization)
COPY pom.xml .
RUN mvn -B dependency:go-offline

# Copy the rest of the project and build
COPY src ./src
RUN mvn clean package -DskipTests

# ========== Stage 2: Create the runtime image ==========
FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy built JAR from the build stage
COPY --from=build /app/target/*.jar app.jar

# Copy certs to /app/certs
COPY src/main/resources/certs /app/certs

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]

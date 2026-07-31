FROM registry.gitlab.com/ty_optimize/optimize/openjdk17:latest

# Install curl for health checks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy the fat JAR
COPY target/dlq-repair-service.jar /app/dlq-repair-service.jar

# Copy configuration file
COPY src/main/resources/dlq-repair.properties /app/dlq-repair.properties

# Create logs directory
RUN mkdir -p /app/logs

# Expose port for health checks (optional - if you add a health endpoint)
EXPOSE 8083

# Run the application
CMD ["java", "-jar", "/app/dlq-repair-service.jar"]
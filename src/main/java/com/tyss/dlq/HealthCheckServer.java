package com.tyss.dlq;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * Simple HTTP server for health checks.
 * Provides /health endpoint for monitoring service status.
 */
public class HealthCheckServer {
    
    private static final Logger log = LoggerFactory.getLogger(HealthCheckServer.class);
    
    private final int port;
    private HttpServer server;
    private volatile boolean healthy = true;
    private volatile String healthMessage = "OK";
    
    public HealthCheckServer(int port) {
        this.port = port;
    }
    
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> {
            try {
                String response;
                int statusCode;
                
                if (healthy) {
                    response = "{\"status\":\"UP\",\"message\":\"" + healthMessage + "\"}";
                    statusCode = 200;
                } else {
                    response = "{\"status\":\"DOWN\",\"message\":\"" + healthMessage + "\"}";
                    statusCode = 503;
                }
                
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(statusCode, response.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } catch (Exception e) {
                log.error("Error handling health check request", e);
            }
        });
        
        server.createContext("/ready", exchange -> {
            try {
                String response = "{\"status\":\"READY\"}";
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } catch (Exception e) {
                log.error("Error handling readiness check request", e);
            }
        });
        
        server.setExecutor(null); // creates a default executor
        server.start();
        log.info("Health check server started on port {}", port);
    }
    
    public void stop() {
        if (server != null) {
            server.stop(0);
            log.info("Health check server stopped");
        }
    }
    
    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }
    
    public void setHealthMessage(String message) {
        this.healthMessage = message;
    }
    
    public boolean isHealthy() {
        return healthy;
    }
}

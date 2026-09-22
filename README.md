# Notification Service

A Spring Boot Kafka **consumer** that listens for employee lifecycle events published by [EmployeeManagement](../EmployeeMangement) and logs a notification for each one. Designed as the downstream half of a simple event-driven microservice pair.

## Tech Stack

- **Java 17**, **Spring Boot 4.1.1**
- **Spring for Apache Kafka** — event consumption
- **Spring Web (MVC)** — included for future REST endpoints (none defined yet)
- **Lombok**
- **Maven** (with wrapper)

## Architecture

```
EmployeeManagement (producer)          NotificationService (this repo)
        │                                       │
        └──► Kafka topic: employee-events ──►  EmployeeConsumer (@KafkaListener)
                                                       │
                                                       └──► console notification log
```

The two services share the same **topic** (`employee-events`) and the same **consumer group** (`notification-group`), but are otherwise independent — each defines its own copy of the `EmployeeEvent` record, decoupled by JSON (see note below).

## What it does

`EmployeeConsumer` listens on topic `employee-events` and, for each `EmployeeEvent` received, prints a notification block to the console based on `eventType`:

- `EMPLOYEE_CREATED` → "Employee created successfully!"
- `EMPLOYEE_UPDATED` → "Employee UPDATED successfully!"
- `EMPLOYEE_DELETED` → "Employee DELETED successfully!"

Example console output:

```
=================================
NOTIFICATION SERVICE
Employee ID   : 1
Employee Name : Abhi
Event Type    : EMPLOYEE_CREATED
Notification  : Employee created successfully!
=================================
```

## Event contract

```java
public record EmployeeEvent(
    Integer employeeId,
    String name,
    String eventType
) {}
```

This mirrors the producer's `EmployeeEvent` field-for-field. Since `spring.json.use.type.headers=false` is set, the consumer ignores the Java class name the producer embeds in Kafka headers and just deserializes the JSON into its own local `EmployeeEvent` record — so the two services don't need to share a JAR or package name, only the same JSON shape.

## Configuration

`application.properties`:

```properties
spring.application.name=notification-service
server.port=8081

spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.consumer.group-id=notification-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JacksonJsonDeserializer

spring.kafka.consumer.properties[spring.json.trusted.packages]=com.abhi.employeemanagement.kafka,com.abhi.notificationservice
spring.kafka.consumer.properties[spring.json.value.default.type]=com.abhi.notificationservice.EmployeeEvent
spring.kafka.consumer.properties[spring.json.use.type.headers]=false
```

`auto-offset-reset=earliest` means a fresh consumer group will replay all retained events on the topic, not just new ones — useful for demos/testing.

## Running locally

```bash
# 1. Start Kafka (e.g. via Docker Compose) on localhost:9092
# 2. Make sure the 'employee-events' topic exists (or let auto-create handle it)
# 3. Run
./mvnw spring-boot:run
```

Then trigger an event by calling the EmployeeManagement API (e.g. `POST /api/employees`) and watch this service's console log.

## Project Structure

```
src/main/java/com/abhi/notificationservice/
├── NotificationServiceApplication.java   Spring Boot entry point
├── EmployeeEvent.java                    Event record (mirrors producer's shape)
└── EmployeeConsumer.java                 @KafkaListener — logs notifications
```

## Possible Next Steps

- Replace `System.out.println` with a real notification channel (email, SMS, push) or at least a proper `Logger`.
- Add a persistence layer to record notification history.
- Add a REST endpoint to query past notifications (the `webmvc` dependency is already present but unused).
- Add error handling / a dead-letter topic for malformed events.
- Add Kafka consumer tests using `spring-boot-starter-kafka-test` (already a dependency).

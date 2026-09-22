package com.abhi.notificationservice;

public record EmployeeEvent(
        Integer employeeId,
        String name,
        String eventType
) {
}
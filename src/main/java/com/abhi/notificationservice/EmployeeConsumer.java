package com.abhi.notificationservice;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class EmployeeConsumer {

    @KafkaListener(
            topics = "employee-events",
            groupId = "notification-group"
    )
    public void consume(EmployeeEvent event) {
        if(event.eventType().equals("EMPLOYEE_CREATED")){
            System.out.println("=================================");
            System.out.println("NOTIFICATION SERVICE");
            System.out.println("Employee ID   : " + event.employeeId());
            System.out.println("Employee Name : " + event.name());
            System.out.println("Event Type    : " + event.eventType());
            System.out.println("Notification  : Employee created successfully!");
            System.out.println("=================================");
        }else if(event.eventType().equals("EMPLOYEE_UPDATED")){
            System.out.println("=================================");
            System.out.println("NOTIFICATION SERVICE");
            System.out.println("Employee ID   : " + event.employeeId());
            System.out.println("Employee Name : " + event.name());
            System.out.println("Event Type    : " + event.eventType());
            System.out.println("Notification  : Employee UPDATED successfully!");
            System.out.println("=================================");
        }else if(event.eventType().equals("EMPLOYEE_DELETED")){
            System.out.println("=================================");
            System.out.println("NOTIFICATION SERVICE");
            System.out.println("Employee ID   : " + event.employeeId());
            System.out.println("Employee Name : " + event.name());
            System.out.println("Event Type    : " + event.eventType());
            System.out.println("Notification  : Employee DELETED successfully!");
            System.out.println("=================================");
        }
    }
}
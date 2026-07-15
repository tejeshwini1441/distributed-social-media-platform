package com.tejeshwini.linkedin.user_service.event;

import lombok.Data;

@Data
public class UserCreatedEvent {
    String name;
    Long userId;
}

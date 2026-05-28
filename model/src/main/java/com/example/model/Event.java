package com.example.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record Event(String id, String name, Instant createdAt) {

    @JsonCreator
    public Event(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name,
            @JsonProperty("createdAt") Instant createdAt) {
        this.id = id;
        this.name = name;
        this.createdAt = createdAt;
    }
}

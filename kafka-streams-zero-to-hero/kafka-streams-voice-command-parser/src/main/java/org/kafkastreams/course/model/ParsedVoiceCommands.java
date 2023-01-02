package org.kafkastreams.course.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder

public class ParsedVoiceCommands {
    private String id;
    private String text;
    private String audioCode;
    private String language;
    private Double probability;
}

package org.kafkastreams.course.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VoiceCommand {
    private String id;
    private byte[] audio;
    private String audioCode;
    private String language;
}

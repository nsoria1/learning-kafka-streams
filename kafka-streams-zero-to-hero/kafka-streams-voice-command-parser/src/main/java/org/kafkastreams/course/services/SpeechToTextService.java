package org.kafkastreams.course.services;

import org.kafkastreams.course.model.ParsedVoiceCommands;
import org.kafkastreams.course.model.VoiceCommand;

public interface SpeechToTextService {

    ParsedVoiceCommands speechToTest(VoiceCommand voiceCommand);
}

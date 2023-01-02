package org.kafkastreams.course.services;

import org.kafkastreams.course.model.ParsedVoiceCommands;

public interface TranslateService {

    ParsedVoiceCommands translate(ParsedVoiceCommands original);
}

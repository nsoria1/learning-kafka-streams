package org.kafkastreams.course;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.kafkastreams.course.model.ParsedVoiceCommands;
import org.kafkastreams.course.model.VoiceCommand;
import org.kafkastreams.course.serdes.JsonSerde;
import org.kafkastreams.course.services.SpeechToTextService;
import org.kafkastreams.course.services.TranslateService;

public class VoiceCommandParserTopology {
    // Define input parameters and constant attributes
    //public static final double certainThreshold = 0.85;
    public static final String VOICE_COMMANDS_TOPIC = "voice-commands";
    public static final String RECOGNIZED_COMMANDS = "recognized-commands";
    public static final String UNRECOGNIZED_COMMANDS = "unrecognized-commands";
    private final SpeechToTextService speechToTextService;
    private final TranslateService translateService;
    private final Double certainThreshold;

    // Define constructor method and what it requires as parameter
    public VoiceCommandParserTopology(SpeechToTextService speechToTextService, TranslateService translateService, Double certainThreshold) {
        this.speechToTextService = speechToTextService;
        this.translateService = translateService;
        this.certainThreshold = certainThreshold;
    }

    // Define method to create a topology
    public Topology createTopology() {
        var streamBuilder = new StreamsBuilder();

        var voiceCommandJsonSerde = new JsonSerde<>(VoiceCommand.class);
        var parsedVoiceCommandJsonSerde = new JsonSerde<>(ParsedVoiceCommands.class);

        var branches = streamBuilder.stream(VOICE_COMMANDS_TOPIC, Consumed.with(Serdes.String(), voiceCommandJsonSerde))
                .filter((key, value) -> value.getAudio().length >= 10)
                // transform the values for each key-value pair with a mocked service
                .mapValues((readOnlyKey, value) -> speechToTextService.speechToTest(value))
                // split probablity
                .split(Named.as("branches-"))
                .branch((key, value) -> value.getProbability() > certainThreshold, Branched.as("recognized"))
                .defaultBranch(Branched.as("unrecognized"));

        branches.get("branches-unrecognized")
                .to(UNRECOGNIZED_COMMANDS, Produced.with(Serdes.String(), parsedVoiceCommandJsonSerde));

        var streamsMap = branches.get("branches-recognized")
                // split into new branches with default name
                .split(Named.as("language-"))
                // lambda function that filters the language attribute "en" in a branch named "english"
                .branch((key, value) -> value.getLanguage().startsWith("en"), Branched.as("english"))
                // remaining of the messages will go to branch "non-english"
                .defaultBranch(Branched.as("non-english"));

        streamsMap.get("language-non-english")
                // based on the streams topology so far, translate the non english messages with the mocked service
                .mapValues((readOnlyKey, value) -> translateService.translate(value))
                // get branch english and merge it with non english
                .merge(streamsMap.get("language-english")) // warning: it must contain the same key and value type
                // send the data to target topic with the serde String and custom JSON Serde
                .to(RECOGNIZED_COMMANDS, Produced.with(Serdes.String(), parsedVoiceCommandJsonSerde));

        return streamBuilder.build();
    }
}

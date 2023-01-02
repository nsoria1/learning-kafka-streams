package org.kafkastreams.course;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kafkastreams.course.model.ParsedVoiceCommands;
import org.kafkastreams.course.model.VoiceCommand;
import org.kafkastreams.course.serdes.JsonSerde;
import org.kafkastreams.course.services.SpeechToTextService;
import org.kafkastreams.course.services.TranslateService;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class VoiceCommandParserTopologyTest {
    @Mock
    private SpeechToTextService speechToTextService;
    @Mock
    private TranslateService translateService;
    private VoiceCommandParserTopology voiceCommandParserTopology;
    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, VoiceCommand> voiceCommandInputTopic;
    private TestOutputTopic<String, ParsedVoiceCommands> recognizedCommandsOutputTopic;
    private TestOutputTopic<String, ParsedVoiceCommands> unrecognizedCommandOutputTopic;


    @BeforeEach
    void setUp() {
        voiceCommandParserTopology = new VoiceCommandParserTopology(speechToTextService, translateService, 0.90);
        var topology = voiceCommandParserTopology.createTopology();

        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        topologyTestDriver = new TopologyTestDriver(topology, props);
        var voiceCommandJsonSerde = new JsonSerde<VoiceCommand>(VoiceCommand.class);
        var parsedVoiceCommandJsonSerde = new JsonSerde<ParsedVoiceCommands>(ParsedVoiceCommands.class);

        voiceCommandInputTopic = topologyTestDriver.createInputTopic(VoiceCommandParserTopology.VOICE_COMMANDS_TOPIC, Serdes.String().serializer(), voiceCommandJsonSerde.serializer());
        recognizedCommandsOutputTopic = topologyTestDriver.createOutputTopic(VoiceCommandParserTopology.RECOGNIZED_COMMANDS, Serdes.String().deserializer(), parsedVoiceCommandJsonSerde.deserializer());
        unrecognizedCommandOutputTopic = topologyTestDriver.createOutputTopic(VoiceCommandParserTopology.UNRECOGNIZED_COMMANDS, Serdes.String().deserializer(), parsedVoiceCommandJsonSerde.deserializer());

    }

    @Test
    @DisplayName("Given an English voice command, When processed correctly Then I receive a ParsedVoiceCommand in the recognnized-commands topic.")
    void testScenario1() {
        // Preconditions (Given)
        byte[] randomBytes = new byte[20];
        new Random().nextBytes(randomBytes);
        var voiceCommand = VoiceCommand.builder()
                .id(UUID.randomUUID().toString())
                .audio(randomBytes)
                .audioCode("FLAC")
                .language("en-US")
                .build();

        var parsedVoiceCommand1 = ParsedVoiceCommands.builder()
                .id(voiceCommand.getId())
                .language("en-US")
                .text("call john")
                .probability(0.98)
                .build();

        given(speechToTextService.speechToTest(voiceCommand)).willReturn(parsedVoiceCommand1);
        // Actions (When)
        voiceCommandInputTopic.pipeInput(voiceCommand);
        // Verifications (Then)
        var parsedVoiceCommand = recognizedCommandsOutputTopic.readRecord().getValue();

        assertTrue(unrecognizedCommandOutputTopic.isEmpty());
        assertEquals(voiceCommand.getId(), parsedVoiceCommand.getId());
        assertEquals(parsedVoiceCommand.getText(), parsedVoiceCommand.getText());
    }

    @Test
    @DisplayName("Given a non-English voice command, When processed correctly Then I receive a ParsedVoiceCommand in the recognnized-commands topic.")
    void testScenario2() {

        // Preconditions (Given)
        byte[] randomBytes = new byte[20];
        new Random().nextBytes(randomBytes);
        var voiceCommand = VoiceCommand.builder()
                .id(UUID.randomUUID().toString())
                .audio(randomBytes)
                .audioCode("FLAC")
                .language("es-AR")
                .build();

        var parsedVoiceCommand1 = ParsedVoiceCommands.builder()
                .id(voiceCommand.getId())
                .text("llamar a Juan")
                .language("es-AR")
                .probability(0.98)
                .build();
        given(speechToTextService.speechToTest(voiceCommand)).willReturn(parsedVoiceCommand1);

        var translatedVoiceCommand = ParsedVoiceCommands.builder()
                .id(voiceCommand.getId())
                .text("call Juan")
                .language("en-US")
                .build();
        given(translateService.translate(parsedVoiceCommand1)).willReturn(translatedVoiceCommand);

        // Actions (When)
        voiceCommandInputTopic.pipeInput(voiceCommand);

        // Verifications (Then)
        var parsedVoiceCommand = recognizedCommandsOutputTopic.readRecord().value();

        assertEquals(voiceCommand.getId(), parsedVoiceCommand.getId());
        assertEquals("call Juan", parsedVoiceCommand.getText());
    }
    @Test
    @DisplayName("Given a non-recognizable voice command, When processed correctly Then I receive a ParsedVoiceCommand in the unrecognnized-commands topic.")
    void testScenario3() {
        // Preconditions (Given)
        byte[] randomBytes = new byte[20];
        new Random().nextBytes(randomBytes);
        var voiceCommand = VoiceCommand.builder()
                .id(UUID.randomUUID().toString())
                .audio(randomBytes)
                .audioCode("FLAC")
                .language("en-US")
                .build();

        var parsedVoiceCommand1 = ParsedVoiceCommands.builder()
                .id(voiceCommand.getId())
                .language("en-US")
                .text("call john")
                .probability(0.75)
                .build();

        given(speechToTextService.speechToTest(voiceCommand)).willReturn(parsedVoiceCommand1);
        // Actions (When)
        voiceCommandInputTopic.pipeInput(voiceCommand);
        // Verifications (Then)
        var parsedVoiceCommand = unrecognizedCommandOutputTopic.readRecord().getValue();

        assertEquals(voiceCommand.getId(), parsedVoiceCommand.getId());
        assertTrue(recognizedCommandsOutputTopic.isEmpty());
        verify(translateService, never()).translate(any(ParsedVoiceCommands.class));
    }

    @Test
    @DisplayName("Given voice command that is too short (less than 10 bytes), When processed correctly Then I don’t receive any command in any of the output topics.")
    void testScenario4() {
        // Preconditions (Given)
        byte[] randomBytes = new byte[9];
        new Random().nextBytes(randomBytes);
        var voiceCommand = VoiceCommand.builder()
                .id(UUID.randomUUID().toString())
                .audio(randomBytes)
                .audioCode("FLAC")
                .language("en-US")
                .build();

        // Actions (When)
        voiceCommandInputTopic.pipeInput(voiceCommand);

        // Verifications (Then)
        assertTrue(recognizedCommandsOutputTopic.isEmpty());
        assertTrue(unrecognizedCommandOutputTopic.isEmpty());

        verify(speechToTextService, never()).speechToTest(any(VoiceCommand.class));
        verify(translateService, never()).translate(any(ParsedVoiceCommands.class));
    }
}
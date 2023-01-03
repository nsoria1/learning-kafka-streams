package com.github.programmingwithmati;

import com.github.programmingwithmati.config.StreamConfiguration;
import com.github.programmingwithmati.topology.BankBalanceTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BankBalanceApp {

    public static void main(String[] args) {
        SpringApplication.run(BankBalanceApp.class, args);
    }
}

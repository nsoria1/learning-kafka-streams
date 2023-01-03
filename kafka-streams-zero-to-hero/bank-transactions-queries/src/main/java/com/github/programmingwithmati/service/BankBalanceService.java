package com.github.programmingwithmati.service;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.topology.BankBalanceTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BankBalanceService {

    private final KafkaStreams kafkaStreams;

    @Autowired
    public BankBalanceService(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    public BankBalance getBankBalanceById(Long bankBalanceId) {
        return getStore().get(bankBalanceId);
    }

    private ReadOnlyKeyValueStore<Long, BankBalance> getStore() {
        return kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                BankBalanceTopology.BANK_BALANCE_STORE,
                QueryableStoreTypes.keyValueStore())
        );

    }
}

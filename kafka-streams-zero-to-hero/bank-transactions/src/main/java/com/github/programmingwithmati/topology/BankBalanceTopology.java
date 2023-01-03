package com.github.programmingwithmati.topology;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.model.BankTransaction;
import com.github.programmingwithmati.model.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class BankBalanceTopology {

    public static final String BANK_TRANSACTIONS = "bank-transactions";
    public static final String BANK_BALANCES = "bank-balances";
    public static final String REJECTED_TRANSACTIONS = "rejected-transactions";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Value Serde
        var bankTransactionJsonSerde = new JsonSerde<BankTransaction>(BankTransaction.class);
        var bankBalanceJsonSerde = new JsonSerde<BankBalance>(BankBalance.class);

        // Create KStream based on the aggregation defined
        var bankBalanceKStream = streamsBuilder.stream(BANK_TRANSACTIONS, Consumed.with(Serdes.Long(), bankTransactionJsonSerde))
                .groupByKey()
                .aggregate(BankBalance::new,
                        (key, value, aggregate) -> aggregate.process(value),
                        Materialized.with(Serdes.Long(), bankBalanceJsonSerde))
                .toStream();

        // Stream bank balance processing to BANK_BALANCE topic with the serdes that applies
        bankBalanceKStream.to(BANK_BALANCES, Produced.with(Serdes.Long(), bankBalanceJsonSerde));

        // For each key get the latest transaction and
        // filter those which state is rejected and send them into REJECTED_TRANSACTIONS topic.
        bankBalanceKStream.mapValues((readOnlyKey, value) -> value.getLatestTransaction())
                .filter((key, value) -> value.getState().equals(BankTransaction.BankTransactionState.REJECTED))
                .to(REJECTED_TRANSACTIONS, Produced.with(Serdes.Long(), bankTransactionJsonSerde));

        return streamsBuilder.build();
    }
}

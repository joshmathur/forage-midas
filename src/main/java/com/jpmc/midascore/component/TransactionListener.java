package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {

        // Step 1: Look up sender and recipient in the database
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // Step 2: Validate - discard if invalid
        if (sender == null || recipient == null) return;
        if (sender.getBalance() < transaction.getAmount()) return;

        // Step 3: Valid transaction - save it and update balances
        transactionRecordRepository.save(new TransactionRecord(sender, recipient, transaction.getAmount()));

        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        userRepository.save(sender);
        userRepository.save(recipient);

        if (sender.getName().equals("waldorf")) {
            System.out.println("WALDORF SENDER balance: " + sender.getBalance());
        }
        if (recipient.getName().equals("waldorf")) {
            System.out.println("WALDORF RECIPIENT balance: " + recipient.getBalance());
        }
    }
}
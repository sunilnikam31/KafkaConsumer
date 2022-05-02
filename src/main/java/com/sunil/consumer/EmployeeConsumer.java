package com.sunil.consumer;

import com.sunil.modal.Employee;
import com.sunil.modal.SequenceGeneratorService;
import com.sunil.repository.EmployeeRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Component
@RestController
@CrossOrigin("*")
public class EmployeeConsumer {
    @Autowired
    EmployeeRepository repository;
    @Autowired
    SequenceGeneratorService sequenceGeneratorService;

    @KafkaListener(topics = "EmployeeTopic")
    public void receive(ConsumerRecord<?, ?> consumerRecord, Acknowledgment acknowledgment) {
        try {
            Employee employee = (Employee) consumerRecord.value();
            employee.setId(sequenceGeneratorService.generateSequence(Employee.SEQUENCE_NAME));
            repository.save(employee);
            acknowledgment.acknowledge();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @GetMapping("/")
    public ResponseEntity<?> getEmployee() {
        return ResponseEntity.ok(this.repository.findAll());
    }
}

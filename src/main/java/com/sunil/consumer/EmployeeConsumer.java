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
import org.springframework.web.bind.annotation.*;

@Component
@RestController
@CrossOrigin
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

    @RequestMapping(value = "/edit/{id}", produces = "application/json", method = RequestMethod.GET)
    public Employee editEmployee(@PathVariable(name = "id") long id) {
        Employee employee = repository.findById(id);
        System.out.println(employee);
        return employee;
    }

    @RequestMapping(value = "/update", method = RequestMethod.PUT, consumes = "application/json", produces = "application/json")
    public Employee updateEmployee(@RequestBody Employee employee) {
        Employee emp = repository.save(employee);
        return emp;
    }

    @RequestMapping(value = "/delete/{id}", produces = "application/json", method = RequestMethod.DELETE)
    public void deleteEmployee(@PathVariable(name = "id") int id) {

        repository.deleteById(id);
    }
}

package cz.demo.eda.process;

import io.camunda.client.annotation.Deployment;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Vstupní bod order-process; při startu nasadí BPMN modely z classpath do Zeebe. */
@SpringBootApplication
@ConfigurationPropertiesScan
@Deployment(resources = "classpath*:bpmn/*.bpmn")
public class OrderProcessApplication {

    /** Spustí aplikaci. */
    public static void main(String[] args) {
        SpringApplication.run(OrderProcessApplication.class, args);
    }
}

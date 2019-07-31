package io.buell.processor;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class ProcessorApp {
    public static void main(String[] args) {
        new SpringApplicationBuilder(ProcessorApp.class).run(args);
    }
}

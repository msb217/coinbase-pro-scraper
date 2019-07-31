package io.buell.processor;

import ch.qos.logback.classic.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class ProcessorControllerRunner implements CommandLineRunner {

    @Autowired
    ApplicationContext context;

    @Autowired
    Environment env;

    public void run(String[] args) {
        Utils.setLoggingLevel(Level.INFO);
    }
}

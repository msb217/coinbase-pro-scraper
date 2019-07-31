package io.buell.processor.controllers;

import io.buell.processor.ProcessorLibrary;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.URISyntaxException;

@RestController
@EnableAutoConfiguration
public class ProcessorAPI {

    @GetMapping("/status")
    public String version() {
        return "hello";
    }

    @GetMapping("/process/{pair}")
    public String process(@PathVariable("pair") String pair) {
        try {
            new ProcessorLibrary().fetch(pair);
        } catch (IOException | URISyntaxException | java.text.ParseException | InterruptedException e) {
            e.printStackTrace();
        }
        return "finished";
    }
}

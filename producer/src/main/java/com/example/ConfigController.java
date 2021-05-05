package com.example;

import com.example.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConfigController {
    @Autowired
    private ProducerConfig config;

    @GetMapping("/config")
    public String getConfig(){
        return config.toString();
    }
}

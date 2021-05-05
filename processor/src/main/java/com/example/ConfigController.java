package com.example;

import com.example.processor.ProcessorConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConfigController {

    @Autowired
    private ProcessorConfig processorConfig;

    @GetMapping("/config")
    public String getConfig(){
        return processorConfig.toString();
    }
}

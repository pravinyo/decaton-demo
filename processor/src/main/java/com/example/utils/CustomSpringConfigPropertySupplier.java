package com.example.utils;

import com.linecorp.decaton.processor.runtime.DynamicProperty;
import com.linecorp.decaton.processor.runtime.Property;
import com.linecorp.decaton.processor.runtime.PropertyDefinition;
import com.linecorp.decaton.processor.runtime.PropertySupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class CustomSpringConfigPropertySupplier implements PropertySupplier, AutoCloseable {

    private Logger logger = LoggerFactory.getLogger(CustomSpringConfigPropertySupplier.class);
    private ConfigProperties config;

    public CustomSpringConfigPropertySupplier(ConfigProperties config) {
        this.config = config;
    }

    @Override
    public <T> Optional<Property<T>> getProperty(PropertyDefinition<T> definition) {
        try{
            DynamicProperty<T> prop = new DynamicProperty<>(definition);
            prop.set((T)config.getProperty(definition));
            logger.info("getting props for {}: {}", definition.name(),prop.value());
            return Optional.of(prop);
        }catch (Exception ex){
            logger.info("Got Exception, No props found");
            return Optional.empty();
        }
    }

    @Override
    public void close() throws Exception {
        logger = null;
        config = null;
    }
}

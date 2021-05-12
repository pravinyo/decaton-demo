package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

public final class JVMDetails {
    private static final Logger LOGGER = LoggerFactory.getLogger(JVMDetails.class);

    public static void printGCDetails(){
        List<GarbageCollectorMXBean> list = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean bean : list){
            LOGGER.info("Name: {}", bean.getName());
            LOGGER.info("Number of collections: {} ", bean.getCollectionCount());
            LOGGER.info("Collection time: {} ms", bean.getCollectionTime());
            LOGGER.info("Pool names");

            for(String name : bean.getMemoryPoolNames()){
                LOGGER.info("\t" + name);
            }

            LOGGER.info("\n");
        }
    }

    public static void getMemoryStatistics() {
        LOGGER.info("Heap Size: {}",Runtime.getRuntime().totalMemory()/1000000);
        LOGGER.info(" Max Heap Size: {}",Runtime.getRuntime().maxMemory()/1000000);
        LOGGER.info(" Free Heap Size: {}",Runtime.getRuntime().freeMemory()/1000000);
    }
}

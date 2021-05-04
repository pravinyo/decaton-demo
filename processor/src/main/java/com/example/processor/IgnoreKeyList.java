package com.example.processor;

import java.util.ArrayList;
import java.util.List;

public final class IgnoreKeyList {
    static List<String> getKeysToIgnore(){
        List<String> keys = new ArrayList<>();
        keys.add("10");
        keys.add("20");
        keys.add("30");
        keys.add("40");
        keys.add("50");
        return keys;
    }
}

package com.example.utils;

import java.util.ArrayList;
import java.util.List;

public final class IgnoreKeyList {
    // Decaton supports defining a list of keys that you want to skip when processing task.
    // The list can be dynamic. It means that you can add or remove keys from the list on the fly.
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

package com.liaobaikai.argparse;

import java.util.HashMap;

public class Namespace {

    private HashMap<String, String> attrs = new HashMap<String, String>();

    public String getString(String dest){
        return null;
    }

    public void put(String key, String value){
        this.attrs.put(key, value);
    }
}

package io.dexterity.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MyConfig {
    public static String path;
    @Value("${local.path}")
    public void setPath(String path){
        MyConfig.path = path;
    }
}

package io.dexterity.util;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Slf4j
public class TimeCountUtil {
    public void countTime(Method method,Object instance,Object... args){
        try {
            long start = System.currentTimeMillis();
            method.invoke(instance,args);
            long end = System.currentTimeMillis()-start;
            log.info("method execute lasts for {} ms",end);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}

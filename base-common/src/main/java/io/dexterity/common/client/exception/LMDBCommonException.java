package io.dexterity.common.client.exception;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LMDBCommonException extends RuntimeException{
    public LMDBCommonException(String message){
        super(message);
    }

}

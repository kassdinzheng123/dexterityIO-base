package io.dexterity.exception;

public class UnexpectedRequestBodyException extends Exception{
    public UnexpectedRequestBodyException(){
        super("Request Body is corrupted,MD5 check Failed");
    }
}

package io.dexterity.exception;

import lombok.Data;

@Data
public class MyException extends RuntimeException {
    private Integer code;

    private String msg;

    public MyException(Integer code, String msg) {
        this.code = code;
        this.msg = msg;
    }
}

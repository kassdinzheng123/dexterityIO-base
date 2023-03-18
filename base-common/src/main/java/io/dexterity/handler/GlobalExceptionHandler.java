package io.dexterity.handler;


import io.dexterity.exception.MyException;
import io.dexterity.po.R;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

@ControllerAdvice
@ResponseBody
public class GlobalExceptionHandler {

    @ExceptionHandler(MyException.class)
    public R<?> handleMyException(MyException e) {
        return new R<>(e.getCode(), e.getMsg());
    }

    // TODO 其他异常处理方法...
}
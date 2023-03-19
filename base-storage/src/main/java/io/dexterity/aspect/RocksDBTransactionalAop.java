package io.dexterity.aspect;

import io.dexterity.util.NoAutoTransactionalUtil;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionStatus;


@Component
@Slf4j
@Aspect
public class RocksDBTransactionalAop {
 
    @Autowired
    private NoAutoTransactionalUtil noAutoTransactionalUtil;
 
    @Around(value = "@annotation(io.dexterity.annotation.RocksDBTransactional)")
    public Object around(ProceedingJoinPoint joinPoint){
        TransactionStatus begin = null;
        try {
            begin = noAutoTransactionalUtil.begin();
            Object result = joinPoint.proceed();//目标方法
            noAutoTransactionalUtil.commit(begin);
            return result;
        } catch (Throwable e) {
            e.printStackTrace();
            if(begin != null){
                noAutoTransactionalUtil.rollback(begin);
                //业务场景：正常业务回滚，但是下面有些这种统一日志都不需要回滚的
                // 记录日志systemParameterInfoService.insertlog();
            }
            return "系统异常";
        }
 
    }
}
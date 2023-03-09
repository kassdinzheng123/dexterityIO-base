package io.dexterity.common.client.aspect;

import io.dexterity.common.client.MultipleLmdb;
import io.dexterity.common.client.annotation.BucketName;
import io.dexterity.common.client.MultipleEnv;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.lmdbjava.Txn;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * 注意！只有在SpringBean中，这个方法的注解才生效
 * 所以，如果不属于SpringBean的lmdb数据库操作，请手动开启事务。
 */
@Aspect
@Data
@Slf4j
@Component
public class LmdbTxn {
    private final ThreadLocal<Txn<ByteBuffer>> txn = new ThreadLocal<>();
    private final ThreadLocal<MultipleEnv> env = new ThreadLocal<>();

    @Pointcut("@annotation(io.dexterity.common.client.annotation.LmdbRead)")
    public void lmdbRead() {}

    @Pointcut("@annotation(io.dexterity.common.client.annotation.LmdbWrite)")
    public void lmdbWrite() {}

    @Around("lmdbRead()")
    public Object handleLmdbRead(ProceedingJoinPoint joinPoint) throws Throwable {
        Object[] args = joinPoint.getArgs(); // 获取目标方法的参数
        MultipleEnv multipleEnv = null;
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (int i = 0; i < args.length; i++) {
            Annotation[] annotations = parameterAnnotations[i];
            for (Annotation annotation : annotations) {
                if (annotation instanceof BucketName) {
                    // parameter i is annotated with @MyAnnotation
                    multipleEnv = MultipleLmdb.envs.get(args[i].toString());
                    // break;
                }
            }
        }
        if (multipleEnv == null){
            throw new LmdbEnvAspectException();
        }
        env.set(multipleEnv);
        Txn<ByteBuffer> txnRead = multipleEnv.getEnv().txnRead();
        txn.set(txnRead);
        Object proceed = null;
        try(txnRead){
            proceed = joinPoint.proceed();
            return proceed;
        }finally {
            txn.remove();
            env.remove();
        }
    }

    @Around("lmdbWrite()")
    public Object handleLmdbWrite(ProceedingJoinPoint joinPoint) throws Throwable {
        Object[] args = joinPoint.getArgs(); // 获取目标方法的参数
        MultipleEnv multipleEnv = null;
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (int i = 0; i < args.length; i++) {
            Annotation[] annotations = parameterAnnotations[i];
            for (Annotation annotation : annotations) {
                if (annotation instanceof BucketName) {
                    // parameter i is annotated with @MyAnnotation
                    multipleEnv = MultipleLmdb.envs.get(args[i].toString());
                    // break;
                }
            }
        }
        if (multipleEnv == null){
            throw new LmdbEnvAspectException();
        }
        env.set(multipleEnv);
        MultipleLmdb.checkAndExpand(multipleEnv.getEnv(),args);

        Object proceed = null;
        try(Txn<ByteBuffer> txnWrite = multipleEnv.getEnv().txnWrite()){
            txn.set(txnWrite);
            proceed = joinPoint.proceed();
            //写事务记得提交
            txn.get().commit();
            return proceed;
        } finally {
            txn.remove();
            env.remove();
        }

    }

    static class LmdbEnvAspectException extends Exception{
        public LmdbEnvAspectException(){
            super("The BucketName annotation should be use to press at least one arg");
        }
    }

}

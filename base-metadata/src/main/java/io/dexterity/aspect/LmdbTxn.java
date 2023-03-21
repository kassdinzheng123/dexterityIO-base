package io.dexterity.aspect;

import io.dexterity.annotation.LmdbTransaction;
import io.dexterity.client.MultipleEnv;
import io.dexterity.client.MultipleLmdb;
import io.dexterity.exception.LMDBCommonException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 注意！只有在SpringBean中，这个方法的注解才生效
 * 所以，如果不属于SpringBean的lmdb数据库操作，请手动开启事务。
 */
@Aspect
@Data
@Slf4j
@Component
public class LmdbTxn {
    private static ConcurrentHashMap<String,Txn<ByteBuffer>> readTxnMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String,Txn<ByteBuffer>> writeTxnMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, MultipleEnv> envMap = new ConcurrentHashMap<>();

    public static final String WRITE = "write";

    public static final String READ = "read";

    public static Txn<ByteBuffer> getWriteTxn(String bucketName){
        return writeTxnMap.get(Thread.currentThread().threadId()+bucketName);
    }

    public static Txn<ByteBuffer> getReadTxn(String bucketName){
        return readTxnMap.get(Thread.currentThread().threadId()+bucketName);
    }

    public static MultipleEnv getEnv(String bucketName){
        return envMap.get(Thread.currentThread().threadId()+bucketName);
    }

    @Pointcut("@annotation(io.dexterity.annotation.LmdbTransaction)")
    public void lmdbTransaction() {}

    @Around("lmdbTransaction()")
    public Object handleLmdbWrite(ProceedingJoinPoint joinPoint) throws Throwable {

        Object[] args = joinPoint.getArgs(); // 获取目标方法的参数
        String bucketName = "";
        List<String> dupNames = new ArrayList<>();
        List<String> unDupNames = new ArrayList<>();


        MultipleEnv multipleEnv = null;
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        LmdbTransaction[] transaction = method.getAnnotationsByType(LmdbTransaction.class);
        LmdbTransaction lmdbTransaction = transaction[0];

        Annotation[][] parameterAnnotations = method.getParameterAnnotations();

        int envParamIndex = -1;
        int txnParamIndex = -1;
        for (int i = 0; i < args.length; i++) {
            Annotation[] annotations = parameterAnnotations[i];
            for (Annotation annotation : annotations) {
                if (annotation.annotationType().getName().equals("io.dexterity.annotation.BucketName")) {
                    bucketName = args[i].toString();
                    multipleEnv = MultipleLmdb.envs.get(bucketName);
                }
                if (annotation.annotationType().getName().equals("io.dexterity.annotation.DupNames"))
                    if (args[i] instanceof List<?> names)
                        for (Object name : names) dupNames.add(name.toString());

                if (annotation.annotationType().getName().equals("io.dexterity.annotation.UnDupNames"))
                    if (args[i] instanceof List<?> names)
                        for (Object name : names) unDupNames.add(name.toString());
            }
            if (args[i].getClass().getName().contains("org.lmdbjava.Txn")){
                txnParamIndex = i;
            }
            else if (args[i].getClass().getName().contains("MultiPleEnv")){
                envParamIndex = i;
            }
        }

        if (multipleEnv == null) {
            throw new LMDBCommonException("BucketName annotation should be used,or this bucket is not exist");
        }

        multipleEnv.initDBs(unDupNames, dupNames);

        String threadId = Thread.currentThread().threadId() + bucketName;
        envMap.put(threadId, multipleEnv);

        Txn<ByteBuffer> txnWrite = multipleEnv.getEnv().txnWrite();
        writeTxnMap.put(threadId, txnWrite);
        Txn<ByteBuffer> txnRead = multipleEnv.getEnv().txnRead();
        readTxnMap.put(threadId, txnRead);

        if (txnParamIndex == -1 || envParamIndex == -1){
            throw new LMDBCommonException("txn and MultipleEnv should be used in the method params");
        }

        if(lmdbTransaction.value().equals(WRITE)){
            args[txnParamIndex] = txnWrite;
        }else{
            args[txnParamIndex] = txnRead;
        }

        args[envParamIndex] = multipleEnv;


        try (txnWrite;txnRead) {
            Object obj = joinPoint.proceed(args);
            txnWrite.commit();
            return obj;
        } catch (Env.MapFullException e) {
            MultipleLmdb.checkAndExpand(multipleEnv.getEnvName(),args);
            return handleLmdbWrite(joinPoint);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            readTxnMap.remove(threadId);
            writeTxnMap.remove(threadId);
        }
    }

}

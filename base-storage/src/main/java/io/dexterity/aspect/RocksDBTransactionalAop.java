package io.dexterity.aspect;

import io.dexterity.po.pojo.RocksDBClient;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.WriteOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
@Component
@Aspect
public class RocksDBTransactionalAop {
    @Autowired
    RocksDBClient rocksDBClient;

    @Pointcut("@annotation(io.dexterity.annotation.RocksDBTransactional)")
    public void logPoint(){}

    @Around("logPoint()")
    public Object around(ProceedingJoinPoint pj) throws Throwable {
        TransactionDB transactionDB = RocksDBClient.getTransactionDB();
        Transaction txn = null;
        Object result=null;
        try{
            txn = transactionDB.beginTransaction(new WriteOptions());
            Object[] args = pj.getArgs();
            for (int i = 0; i < args.length; i++) {
                if (args[i] == null) {
                    args[i] = txn; // 对参数值为null的参数进行赋值
                }
            }
            result = pj.proceed(args);
            txn.commit();
        }catch (Throwable e){
            txn.rollback();
        }
        return result;
    }
}
package io.dexterity.util;
//编程事务，自定义，自己控制 begin  commit  rollback
//缺点，代码繁杂，优点：扩展性更强，自己控制什么时候commit  和rollback
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
 
@Component
public class NoAutoTransactionalUtil {
    @Autowired
    private DataSourceTransactionManager dataSourceTransactionManager;
 
    /**
     * 开启事务
     * @return
     */
    public TransactionStatus begin(){
        return dataSourceTransactionManager.getTransaction(new DefaultTransactionAttribute());
    }
 
    /**
     *  提交事务
     * @return
     */
    public void commit(TransactionStatus transactionStatus){
        dataSourceTransactionManager.commit(transactionStatus);
    }
 
    /**
     * 回滚事务
     * @return
     */
    public void rollback(TransactionStatus transactionStatus){
        dataSourceTransactionManager.rollback(transactionStatus);
    }
}
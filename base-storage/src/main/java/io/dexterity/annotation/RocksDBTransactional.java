package io.dexterity.annotation;//自定义事务管理器
 
import java.lang.annotation.*;
 
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface RocksDBTransactional {
}
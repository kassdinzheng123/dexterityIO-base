package io.dexterity;

import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@Slf4j
//@EnableMethodCache(basePackages = "io.dexterity")
@SpringBootApplication(scanBasePackages = {"io.dexterity"})
@MapperScan({"io.dexterity.bucket.dao"})
public class DexterityIOEntrance {
    public static void main(String[] args) {
        SpringApplication.run(DexterityIOEntrance.class);
    }

    /**
     * 应用关闭前关闭一切Env
     */
//    private void envDestroy(){
//        envs.forEach(
//                (key,value)->{
//                    if (!value.getEnv().isClosed()){
//                        try{
//                            value.getEnv().close();
//                            log.info("LMDB destroy : Env {} is closed",key);
//                        }catch (Env.AlreadyClosedException e){
//                            log.warn("LMDB destroy Exception: Env {} is closed twice",key);
//                        }catch (Exception e){
//                            log.info("LMDB destroy Exception: Env {} failed to close,the data might lose",key);
//                        }
//                    }
//                }
//        );
//    }
//
//    @Override
//    public int getExitCode() {
//        envDestroy();
//        return 0;
//    }

}
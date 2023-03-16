package io.dexterity;

import io.dexterity.config.MyConfig;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;

//@Slf4j
//@EnableMethodCache(basePackages = "io.dexterity")
@SpringBootApplication
public class DexterityIOEntrance {
    public static void main(String[] args) {
        SpringApplication.run(DexterityIOEntrance.class);
    }
    @Autowired
    private MyConfig myConfig;
    @PostConstruct
    public void init() {
        File folder = new File(MyConfig.path+"Resource\\lmdb");
        if(!folder.exists()) {
            folder.mkdirs();// 创建文件夹
        }
    }
    /*
      应用关闭前关闭一切Env
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
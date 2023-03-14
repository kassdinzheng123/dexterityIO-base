//package io.dexterity.common.config.cache;
//
//import com.alicp.jetcache.CacheBuilder;
//import com.alicp.jetcache.anno.CacheConsts;
//import com.alicp.jetcache.anno.config.EnableMethodCache;
//import com.alicp.jetcache.anno.support.GlobalCacheConfig;
//import com.alicp.jetcache.anno.support.JetCacheBaseBeans;
//import com.alicp.jetcache.embedded.LinkedHashMapCacheBuilder;
//import com.alicp.jetcache.support.FastjsonKeyConvertor;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Import;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
//@Configuration
//@EnableMethodCache(basePackages = "io.dexterity")
//@Import(JetCacheBaseBeans.class) //need since jetcache 2.7+
//public class JetCacheConfig {
//
//    @Bean
//    public GlobalCacheConfig config() {
//        // public GlobalCacheConfig config(SpringConfigProvider configProvider, Pool<Jedis> pool){ // for jetcache <=2.5
//        Map<String, CacheBuilder> localBuilders = new HashMap<>();
//        var localBuilder = LinkedHashMapCacheBuilder
//                .createLinkedHashMapCacheBuilder()
//                .expireAfterWrite(1, TimeUnit.DAYS)
//                .limit(1000)
//                .keyConvertor(FastjsonKeyConvertor.INSTANCE);
//        localBuilders.put(CacheConsts.DEFAULT_AREA, localBuilder);
//
//
//        GlobalCacheConfig globalCacheConfig = new GlobalCacheConfig();
//        globalCacheConfig.setLocalCacheBuilders(localBuilders);
//        globalCacheConfig.setStatIntervalMinutes(15);
//
//        return globalCacheConfig;
//    }
//}
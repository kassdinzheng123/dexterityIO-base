package io.dexterity.config.cache;

//@Configuration
//@EnableMethodCache(basePackages = "io.dexterity")
//@Import(JetCacheBaseBeans.class) //need since jetcache 2.7+
public class JetCacheConfig {

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
}
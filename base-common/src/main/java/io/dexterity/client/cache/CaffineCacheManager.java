package io.dexterity.client.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.Cache;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.support.AbstractCacheManager;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class CaffineCacheManager extends AbstractCacheManager {

    private final ConcurrentMap<String, Cache> cacheMap = new ConcurrentHashMap<>(16);

    public CaffineCacheManager() {
        super();
    }

    @Override
    @NonNull
    protected Collection<? extends Cache> loadCaches() {
        Collection<String> cacheNames = getCacheNames();
        Assert.notEmpty(cacheNames, "Cache names must not be empty");

        for (String cacheName : cacheNames) {
            Cache cache = getCache(cacheName);
            if (cache == null) {
                addCache(createCaffeineCache(cacheName));
            }
        }

        return cacheMap.values();
    }

    @Override
    public Cache getCache(@NonNull String name) {
        Cache cache = cacheMap.get(name);
        if (cache == null) {
            cache = createCaffeineCache(name);
            cacheMap.put(name, cache);
        }
        return cache;
    }

    @Override
    protected Cache getMissingCache(@NonNull String name) {
        return createCaffeineCache(name);
    }

    public void addCache(CaffeineCache cache) {
        this.cacheMap.put(cache.getName(), cache);
    }

    private CaffeineCache createCaffeineCache(String name) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder()
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .maximumSize(100);

        // Add Bloom filter to cache
//        builder.recordStats().;
        return new CaffeineCache(name, builder.build(), false);
    }

}

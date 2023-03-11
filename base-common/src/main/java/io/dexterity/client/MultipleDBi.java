package io.dexterity.client;

import com.alicp.jetcache.Cache;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.dexterity.exception.LMDBCommonException;
import io.dexterity.util.EncryptionUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.lmdbjava.*;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author haoran
 * LMDB 客户端
 * 包含一个DB实例，以及实例所属的Env
 */
@Slf4j
public class MultipleDBi {

    //DB实例
    private final Dbi<ByteBuffer> db;

    //所属的ENV
    private final String envName;

    private final Gson gson = new Gson();

    private final Env<ByteBuffer> env;

    /**
     *
     */
    private final Cache<String,List<String>> cache;

    private final EncryptionUtils encryptionUtils;

    @Getter
    private final String password;

    public MultipleDBi(Dbi<ByteBuffer> db, String envName,String password,Cache<String,List<String>> cache) {
        this.db = db;
        this.env = MultipleLmdb.envs.get(envName).getEnv();
        this.envName = envName;
        this.encryptionUtils = new EncryptionUtils(password);
        this.password = password;
        this.cache = cache;
    }

    /**
     * String 转 ByteBuffer
     *
     * @param s 要转的字符串
     * @return 转换结果
     */
    private ByteBuffer stringToBytes(String s) {
        String encrypted = encryptionUtils.encrypt(s);
//        log.info("encrypt : {} -> {}",s,encrypted);
        byte[] bytes = encrypted.getBytes();
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        //将字符串转成ByteBuffer
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    private String bytesToString(ByteBuffer buffer) {
        Charset charset = StandardCharsets.UTF_8;
        CharBuffer charBuffer = charset.decode(buffer);
        return encryptionUtils.decrypt(charBuffer.toString());
    }

    /**
     * 精准获取一个KEY
     *
     * @param key key
     * @return 对象的JSON字符串
     */
    public String get(String key) {

        List<String> list = cache.get(new String(db.getName()) + key);
        if (list != null) {
            if (list.size() > 1) throw new InvalidMethodException();
            return list.get(0);
        }

        ByteBuffer byteBuffer = stringToBytes(key);
        ByteBuffer res;
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            res = db.get(txn, byteBuffer);
        }

        if (res == null) return null;

        String resString = bytesToString(res);
        putCache(key,resString);

        return resString;
    }

    /**
     * 精准获取一个KEY
     *
     * @param key key
     * @return 对象的JSON字符串
     */
    public String get(String key, Txn<ByteBuffer> txn) {
        List<String> list = cache.get(new String(db.getName()) + key);
        if (list != null) {
            if (list.size() > 1) throw new InvalidMethodException();
            return list.get(0);
        }

        ByteBuffer byteBuffer = stringToBytes(key);
        ByteBuffer res;
        res = db.get(txn, byteBuffer);
        if (res == null) return null;

        String resString = bytesToString(res);
        putCache(key,resString);

        return resString;
    }

    /**
     * 如果要插入JSON
     *
     * @param key    key
     * @param object 对象
     * @param <T>    对象类型
     */
    public <T> void putJsonObject(String key, T object) {
        String s = gson.toJson(object);
        this.put(key, s);
    }

    /**
     * 如果要插入JSON
     *
     * @param key    key
     * @param object 对象
     * @param <T>    对象类型
     */
    public <T> void putJsonObject(String key, T object, Txn<ByteBuffer> txn) {
        String s = gson.toJson(object);
        this.put(key, s, txn);
    }

    /**
     * 获取一个对象，转换为需要的类型
     *
     * @param key key
     * @param <T> 类型
     */
    public <T> T getAsObject(String key) {
        String s = this.get(key);
        Type type = new TypeToken<T>() {}.getType();
        return gson.fromJson(s, type);
    }


    /**
     * 获取一个对象，转换为需要的类型
     *
     * @param key key
     * @param <T> 类型
     */
    public <T> T getAsObject(String key, Txn<ByteBuffer> txn) {
        String s = this.get(key, txn);
        Type type = new TypeToken<T>() {
        }.getType();
        return gson.fromJson(s, type);
    }


    /**
     * 获取一堆对象，转换为需要的类型
     *
     * @param keys key
     * @param <T>  类型
     */
    public <T> Map<String, T> getAsObjects(String... keys) {
        Map<String, String> patch = this.getPatch(keys);
        Map<String, T> res = new HashMap<>();
        patch.forEach(
                (key, value) -> {
                    Type type = new TypeToken<T>() {
                    }.getType();
                    res.put(key, gson.fromJson(value, type));
                }
        );
        return res;
    }

    /**
     * 获取一堆对象，转换为需要的类型
     *
     * @param keys key
     * @param <T>  类型
     */
    public <T> Map<String, T> getAsObjects(Txn<ByteBuffer> txn, String... keys) {
        Map<String, String> patch = this.getPatch(txn, keys);
        Map<String, T> res = new HashMap<>();
        patch.forEach(
                (key, value) -> {
                    Type type = new TypeToken<T>() {
                    }.getType();
                    res.put(key, gson.fromJson(value, type));
                }
        );
        return res;
    }

    /**
     * 获取一堆对象，转换为需要的类型
     *
     * @param keys key
     * @param <T>  类型
     */
    public <T> Map<String, T> getAsObjects(Collection<String> keys) {
        Map<String, String> patch = this.getPatch(keys);
        Map<String, T> res = new HashMap<>();
        patch.forEach(
                (key, value) -> {
                    Type type = new TypeToken<T>() {
                    }.getType();
                    res.put(key, gson.fromJson(value, type));
                }
        );
        return res;
    }

    /**
     * 获取一堆对象，转换为需要的类型
     *
     * @param keys key
     * @param <T>  类型
     */
    public <T> Map<String, T> getAsObjects(Txn<ByteBuffer> txn, Collection<String> keys) {
        Map<String, String> patch = this.getPatch(txn, keys);
        Map<String, T> res = new HashMap<>();
        patch.forEach(
                (key, value) -> {
                    Type type = new TypeToken<T>() {
                    }.getType();
                    res.put(key, gson.fromJson(value, type));
                }
        );
        return res;
    }

    /**
     * 如果要插入很多对象
     *
     * @param objects 对象们
     * @param <T>     对象泛型
     */
    public <T> void putAllJsonObject(Map<String, List<T>> objects) {
        HashMap<String, List<String>> putMap = new HashMap<>();
        objects.forEach((key, value) -> {
            value.forEach(
                    v -> {
                        putMap.putIfAbsent(key, new ArrayList<>());
                        putMap.get(key).add(gson.toJson(v));
                    }
            );
        });
        this.putAll(putMap);
    }

    /**
     * 如果要插入很多对象
     *
     * @param objects 对象们
     * @param <T>     对象泛型
     */
    public <T> void putAllJsonObject(Txn<ByteBuffer> txn, Map<String, List<T>> objects) {
        HashMap<String, List<String>> putMap = new HashMap<>();
        objects.forEach((key, value) -> value.forEach(
                v -> {
                    putMap.putIfAbsent(key, new ArrayList<>());
                    putMap.get(key).add(gson.toJson(v));
                }
        ));
        this.putAll(txn, putMap);
    }

    static class InvalidMethodException extends RuntimeException{
        public InvalidMethodException(){
            super("Lmdb: getPatch Method should not used for multiPie key-value entry,use getDuplicatedData instead");
        }
    }

    /**
     * 如果要获取很多Value
     *
     * @param keys 一堆key
     * @return 结果的key-value map
     */
    public Map<String, String> getPatch(Collection<String> keys) {
        Map<String, String> resList = new HashMap<>();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            doGetPatch(txn, keys, resList);
        }
        if (resList.size() != keys.size())
            throw new LMDBCommonException("Lmdb patch query error:resList's length doesn't match keys");
        return resList;
    }

    /**
     * 如果要获取很多Value
     *
     * @param keys 一堆key
     * @return 结果的key-value map
     */
    public Map<String, String> getPatch(Txn<ByteBuffer> txn, Collection<String> keys) {
        Map<String, String> resList = new HashMap<>();
        doGetPatch(txn, keys, resList);
        if (resList.size() != keys.size())
            throw new LMDBCommonException("Lmdb patch query error:resList's length doesn't match keys");
        return resList;
    }

    private void doGetPatch(Txn<ByteBuffer> txn, Collection<String> keys, Map<String, String> resList) {
        for (var s : keys) {
            List<String> list = cache.get(s);
            if (list != null && list.size() == 1){
                resList.put(s,list.get(0));
                continue;
            }else if (list != null && list.size() > 1){
                throw new InvalidMethodException();
            }

            ByteBuffer byteBuffer = stringToBytes(s);
            ByteBuffer res = db.get(txn, byteBuffer);
            if (res == null) resList.put(s, null);
            else {
                String value = bytesToString(res);
                putCache(s,value);
                resList.put(s, value);
            }
        }
    }

    /**
     * 如果要获取很多Value
     *
     * @param keys 一堆key
     * @return 结果的key-value map
     */
    public Map<String, String> getPatch(Txn<ByteBuffer> txn, String... keys) {
        Map<String, String> resList = new HashMap<>();
        doGetPatch(txn, List.of(keys),resList);
        if (resList.size() != keys.length)
            throw new LMDBCommonException("Lmdb patch query error:resList's length doesn't match keys");
        return resList;
    }

    /**
     * 如果要获取很多Value
     *
     * @param keys 一堆key
     * @return 结果的key-value map
     */
    public Map<String, String> getPatch(String... keys) {
        Map<String, String> resList = new HashMap<>();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            doGetPatch(txn,List.of(keys),resList);
        }
        if (resList.size() != keys.length)
            throw new LMDBCommonException("Lmdb patch query error:resList's length doesn't match keys");
        return resList;
    }

    /**
     * 进行前缀搜索，返回一个由key名称和keyVal对组成的集合
     *
     * @param prefix 前缀名称
     * @return 返回一个Map
     */
    public Map<String, CursorIterable.KeyVal<ByteBuffer>> prefixSearch(String prefix) {
        //TODO 测试 和 缓存（由于暂时没用到这个特性，搁置）
        ByteBuffer prefixBuffer = stringToBytes(prefix);
        Map<String, CursorIterable.KeyVal<ByteBuffer>> res = new HashMap<>();
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            iteratePrefix(txn, prefix, prefixBuffer, res);
        }
        return res;
    }

    /**
     * 进行前缀搜索，返回一个由key名称和keyVal对组成的集合
     *
     * @param prefix 前缀名称
     * @return 返回一个Map
     */
    public Map<String, CursorIterable.KeyVal<ByteBuffer>> prefixSearch(Txn<ByteBuffer> txn, String prefix) {
        //TODO 测试 和 缓存（由于暂时没用到这个特性，搁置）
        ByteBuffer prefixBuffer = stringToBytes(prefix);
        Map<String, CursorIterable.KeyVal<ByteBuffer>> res = new HashMap<>();
        iteratePrefix(txn, prefix, prefixBuffer, res);
        return res;
    }

    /**
     * 进行前缀迭代
     *
     * @param txn          事务
     * @param prefix       前缀
     * @param prefixBuffer 前缀的buffer
     * @param res          结果
     */
    private void iteratePrefix(Txn<ByteBuffer> txn,
                               String prefix,
                               ByteBuffer prefixBuffer,
                               Map<String, CursorIterable.KeyVal<ByteBuffer>> res) {
        for (CursorIterable.KeyVal<ByteBuffer> next :
                db.iterate(txn, KeyRange.atLeast(prefixBuffer))) {
            ByteBuffer keyBuf = next.key();
            byte[] keyBytes = new byte[keyBuf.remaining()];
            keyBuf.get(keyBytes);

            String key = new String(keyBytes);
            if (!key.startsWith(prefix)) {
                break;
            }
            res.put(key, next);
        }
    }


    /**
     * 看上去就性能不会好的东西
     * 主要是在put的时候使该value对应的某个范围查询的缓存过期
     * @param value put的值
     */
    private void removeRange(String value){

        Set<String> removeU = new HashSet<>();
        Set<String> removeL = new HashSet<>();
        List<String> listU = cache.get("ub-pos");
        List<String> listL = cache.get("lb-pos");
        if (listL == null) listL = new ArrayList<>();
        if (listU == null) listU = new ArrayList<>();
        for (String nextUb : listU) {
            if (nextUb.compareTo(value) >= 0) {
                cache.remove(new String(db.getName()) + "ranged-ub-" + nextUb);
                removeU.add(nextUb);
            }
            for (String nextLb : listL) {
                if (nextLb.compareTo(value) <= 0) {
                    cache.remove(new String(db.getName()) + "ranged-lb-" + nextLb);
                    cache.remove(new String(db.getName()) + "ranged-lb-" + nextLb + "-ub-" + nextUb);
                    removeL.add(nextLb);
                }
            }
        }
        listU.removeAll(removeU);
        listL.removeAll(removeL);
        putCache("ub-pos",listU);
        putCache("lb-pos",listL);
    }

    private void putCache(String key,String value){
        removeRange(value);
        key = new String(db.getName()) + "-"+ key;
        if(!cache.putIfAbsent(key, Collections.singletonList(value))){
            List<String> list = cache.get(key);
            if (list == null) list = new ArrayList<>();
            else list = new ArrayList<>(list);
            list.add(value);
            cache.put(key,list);
        }
    }

    private void putCache(String key,List<String> value){
        value.forEach(this::removeRange);
        key = new String(db.getName()) + "-" +key;
        if(!cache.putIfAbsent(key, new ArrayList<>(value))){
            List<String> list = cache.get(key);
            if (list == null) list = new ArrayList<>();
            else list = new ArrayList<>(list);
            list.addAll(value);
            cache.put(key,list);
        }
    }

    /**
     * 插入一个键值对
     *
     * @param key   key String类型
     * @param value value String类型
     */
    public void put(String key, String value) {
        ByteBuffer keyBuffer = stringToBytes(key);
        ByteBuffer valueBuffer = stringToBytes(value);
        db.put(keyBuffer, valueBuffer);
        putCache(key,value);
    }

    /**
     * 插入一个键值对
     *
     * @param key   key String类型
     * @param value value String类型
     */
    public void put(String key, String value, Txn<ByteBuffer> txn) {
        ByteBuffer keyBuffer = stringToBytes(key);
        ByteBuffer valueBuffer = stringToBytes(value);
        db.put(txn, keyBuffer, valueBuffer);
        putCache(key,value);
    }

    /**
     * 批量插入
     *
     * @param map key-value 对
     */
    public void putAll(Txn<ByteBuffer> txn, @NotNull Map<String, List<String>> map) {
        try(Txn<ByteBuffer> ct = env.txn(txn)){
            Cursor<ByteBuffer> c = db.openCursor(ct);
            for (var entry : map.entrySet()) {
                ByteBuffer keyBuffer = stringToBytes(entry.getKey());
                for (var a : entry.getValue()) {
                    ByteBuffer valueBuffer = stringToBytes(a);
                    c.put(keyBuffer, valueBuffer);
                    c.next();
                }
            }
            c.close();
            ct.commit();
        }

        for (var entry : map.entrySet()) {
            putCache(entry.getKey(),entry.getValue());
        }
    }



    /**
     * 批量插入
     *
     * @param map key-value 对
     */
    public void putAll(@NotNull Map<String, List<String>> map) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            final Cursor<ByteBuffer> c = db.openCursor(txn);

            map.forEach(
                    (key, value) -> {
                        ByteBuffer keyBuffer = stringToBytes(key);
                        value.forEach(
                                v -> {
                                    ByteBuffer valueBuffer = stringToBytes(v);
                                    c.put(keyBuffer, valueBuffer);
                                    c.next();
                                }
                        );

                    }
            );

            c.close();
            txn.commit();
        }
        for (var entry : map.entrySet()) {
            putCache(entry.getKey(),entry.getValue());
        }
    }

    /**
     * 检索重复Key
     *
     * @param key String类型的Key
     * @return String类型的List 查询结果
     */
    public List<String> getDuplicatedData(String key) {
        ByteBuffer keyBuffer = stringToBytes(key);
        List<String> resultList = new ArrayList<>();
        List<String> list = cache.get(new String(db.getName())+key);
        if (list != null && !list.isEmpty()) return list;

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn, KeyRange.closed(keyBuffer, keyBuffer))) {
                String s = bytesToString(byteBufferKeyVal.val());
                resultList.add(s);
            }
        }

        putCache(key,resultList);
        return resultList;
    }

    /**
     * 检索重复Key
     *
     * @param key String类型的Key
     * @return String类型的List 查询结果
     */
    public List<String> getDuplicatedData(String key, Txn<ByteBuffer> txn) {
        ByteBuffer keyBuffer = stringToBytes(key);
        List<String> list = cache.get(new String(db.getName())+key);
        if (list != null && !list.isEmpty()) return list;
        List<String> resultList = new ArrayList<>();
        for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn, KeyRange.closed(keyBuffer, keyBuffer))) {
            String s = bytesToString(byteBufferKeyVal.val());
            resultList.add(s);
        }

        putCache(key,resultList);
        return resultList;
    }


    /**
     * 检索一个范围以内的数据
     * 携带父事务
     * 重复Key检索
     * 全额检索不走缓存！
     * @param ub key的上界 可以为空，代表无上界 注意：为小于等于
     * @param lb key的下界 可以为空，代表无下届 注意：为大于等于
     * @return 符合条件的所有值
     */
    public List<String> getRangedDuplicatedData(String lb, String ub) {

        //全额返回
        if (ub == null && lb == null) {

            List<String> resultList = new ArrayList<>();
            try (Txn<ByteBuffer> txn = env.txnRead()) {
                for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn)) {
                    String s = bytesToString(byteBufferKeyVal.val());
                    resultList.add(s);
                }
            }
            return resultList;
        }

        //小于等于
        if (lb == null) {
            ByteBuffer ubBuffer = stringToBytes(ub);
            List<String> resultList = new ArrayList<>();

            List<String> list = cache.get(new String(db.getName()) + "ranged-ub-" + ub);
            if (list != null && list.isEmpty()) return list;

            try (Txn<ByteBuffer> txn = env.txnRead()) {
                for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn, KeyRange.atMost(ubBuffer))) {
                    String s = bytesToString(byteBufferKeyVal.val());
                    resultList.add(s);
                }
            }

            putCache("ub-pos",ub);
            putCache("ranged-ub-"+ub,resultList);
            return resultList;
        }

        //大于等于
        if (ub == null) {
            ByteBuffer lbBuffer = stringToBytes(lb);
            List<String> resultList = new ArrayList<>();

            List<String> list = cache.get(new String(db.getName()) + "ranged-lb-" + lb);
            if (list != null && list.isEmpty()) return list;

            try (Txn<ByteBuffer> txn = env.txnRead()) {
                for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn, KeyRange.atLeast(lbBuffer))) {
                    String s = bytesToString(byteBufferKeyVal.val());
                    resultList.add(s);
                }
            }

            putCache("ranged-ub-"+lb,resultList);
            putCache("lb-pos",lb);
            return resultList;
        }


        //上下界都有
        ByteBuffer ubBuffer = stringToBytes(ub);
        ByteBuffer lbBuffer = stringToBytes(lb);
        List<String> resultList = new ArrayList<>();

        String key = new String(db.getName()) + "ranged-lb-" + lb + "-ub-" + ub;
        List<String> list = cache.get(key);
        if (list != null && list.isEmpty()) return list;

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn, KeyRange.closed(ubBuffer, lbBuffer))) {
                String s = bytesToString(byteBufferKeyVal.val());
                resultList.add(s);
            }
        }

        putCache(key,resultList);
        putCache("lb-pos",lb);
        putCache("ub-pos",ub);
        return resultList;
    }


    /**
     * 检索一个范围以内的数据
     * 重复Key检索
     *
     * @param ub key的上界 可以为空，代表无上界 注意：为小于等于
     * @param lb key的下界 可以为空，代表无下届 注意：为大于等于
     * @return 符合条件的所有值
     */
    public List<String> getRangedDuplicatedData(Txn<ByteBuffer> parent, String lb, String ub) {


        //全额返回
        if (ub == null && lb == null) {


            List<String> resultList = new ArrayList<>();
            try (Txn<ByteBuffer> txn = env.txn(parent)) {
                for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn)) {
                    String s = bytesToString(byteBufferKeyVal.val());
                    resultList.add(s);
                }
            }
            return resultList;
        }

        //小于等于
        if (lb == null) {
            ByteBuffer ubBuffer = stringToBytes(ub);
            List<String> resultList = new ArrayList<>();

            List<String> list = cache.get(new String(db.getName()) + "ranged-ub-" + ub);
            if (list != null && list.isEmpty()) return list;

            try (Txn<ByteBuffer> txn = env.txn(parent)) {
                for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn, KeyRange.atMost(ubBuffer))) {
                    String s = bytesToString(byteBufferKeyVal.val());
                    resultList.add(s);
                }
            }

            putCache("ranged-ub-"+ub,resultList);
            return resultList;
        }

        //大于等于
        if (ub == null) {
            ByteBuffer lbBuffer = stringToBytes(lb);
            List<String> resultList = new ArrayList<>();

            List<String> list = cache.get(new String(db.getName()) + "ranged-lb-" + lb);
            if (list != null && list.isEmpty()) return list;

            try (Txn<ByteBuffer> txn = env.txn(parent)) {
                for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn, KeyRange.atLeast(lbBuffer))) {
                    String s = bytesToString(byteBufferKeyVal.val());
                    resultList.add(s);
                }
            }

            putCache("ranged-ub-"+lb,resultList);

            return resultList;
        }


        //上下界都有
        ByteBuffer ubBuffer = stringToBytes(ub);
        ByteBuffer lbBuffer = stringToBytes(lb);
        List<String> resultList = new ArrayList<>();

        String key = new String(db.getName()) + "ranged-lb-" + lb + "-ub-" + ub;
        List<String> list = cache.get(key);
        if (list != null && list.isEmpty()) return list;

        try (Txn<ByteBuffer> txn = env.txn(parent)) {
            for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn, KeyRange.closed(ubBuffer, lbBuffer))) {
                String s = bytesToString(byteBufferKeyVal.val());
                resultList.add(s);
            }
        }
        putCache(key,resultList);
        return resultList;
    }

    /**
     * 调试用方法
     *
     * @param key String类型的Key
     * @return String类型的List 查询结果
     */
    public String getAll(String key) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            List<String> resultList = new ArrayList<>();
            ByteBuffer byteBuffer = db.get(txn, stringToBytes(key));
            return bytesToString(byteBuffer);
        }
    }

    /**
     * 删除重复Key中，某些value为xx的指
     *
     * @param key   String类型的Key
     * @param value String类型的value
     */
    public void deleteFromDuplicatedData(String key, String value) {
        ByteBuffer keyBuffer = stringToBytes(key);
        ByteBuffer valueBuffer = stringToBytes(value);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            CursorIterable<ByteBuffer> iterate = db.iterate(txn, KeyRange.closed(keyBuffer, keyBuffer));
            Iterator<CursorIterable.KeyVal<ByteBuffer>> iterator = iterate.iterator();
            while (iterator.hasNext()) {
                if (iterator.next().val().equals(valueBuffer)) iterator.remove();
            }
        }
        cache.remove(new String(db.getName())+key);
    }

    /**
     * 删除重复Key中，某些value为xx的指
     *
     * @param key   String类型的Key
     * @param value String类型的value
     */
    public void deleteFromDuplicatedData(String key, String value, Txn<ByteBuffer> txn) {
        ByteBuffer keyBuffer = stringToBytes(key);
        ByteBuffer valueBuffer = stringToBytes(value);
        CursorIterable<ByteBuffer> iterate = db.iterate(txn, KeyRange.closed(keyBuffer, keyBuffer));
        Iterator<CursorIterable.KeyVal<ByteBuffer>> iterator = iterate.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().val().equals(valueBuffer)) iterator.remove();
        }
        cache.remove(new String(db.getName())+key);
    }


    /**
     * 删除一个键值对
     *
     * @param key key String类型
     */
    public void delete(String key) {
        ByteBuffer keyBuffer = stringToBytes(key);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            db.delete(keyBuffer);
            txn.commit();
        }
        cache.remove(new String(db.getName())+key);
    }


    /**
     * 删除以prefix为前缀的键值对
     *
     * @param prefix 前缀
     */
    public void deletePrefix(String prefix) {
        //TODO prefix 系列操作
        Map<String, CursorIterable.KeyVal<ByteBuffer>> stringKeyValMap = prefixSearch(prefix);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            stringKeyValMap.forEach((key, value) -> db.delete(value.key()));
            txn.commit();
        }
    }


}

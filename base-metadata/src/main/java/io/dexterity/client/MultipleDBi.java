package io.dexterity.client;

import cn.hutool.core.map.MapUtil;
import com.alicp.jetcache.Cache;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.dexterity.exception.LMDBCommonException;
import io.dexterity.util.EncryptionUtils;
import io.dexterity.util.securtiy.Key;
import io.dexterity.util.securtiy.fast.FastOpeCipher;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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
    public final Dbi<ByteBuffer> db;

    //所属的ENV
    private final String envName;

    private final Gson gson = new Gson();

    public final Env<ByteBuffer> env;

    private final EncryptionUtils encryptionUtils;

    //Order-Preserve-encryption
    private final Key key;

    @Getter
    private final String password;

    @Getter
    private final boolean isDup;

    public MultipleDBi(Dbi<ByteBuffer> db, String envName, String password, Cache<String, List<String>> cache,boolean isDup) {
        this.db = db;
        this.env = MultipleLmdb.envs.get(envName).getEnv();
        this.envName = envName;
        this.encryptionUtils = new EncryptionUtils(password);
        this.password = password;
        FastOpeCipher fastOpeCipher = new FastOpeCipher();
        String s = "0.04677844849377971,0.03153262861848518,193006692460445328,17172942787053";
        this.key = fastOpeCipher.generateKey(s);
        this.isDup = isDup;
    }

    /**
     * String 转 ByteBuffer
     *
     * @param s 要转的字符串
     * @return 转换结果
     */
    public ByteBuffer byteKey(String s) {
        byte[] bytes = key.encryptString(s);
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        //将字符串转成ByteBuffer
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    public ByteBuffer byteValue(String s) {
        String encrypted = encryptionUtils.encrypt(s);
        byte[] bytes = encrypted.getBytes();
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        //将字符串转成ByteBuffer
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    private static byte[] bufferToByte(ByteBuffer buffer) {
        buffer.rewind();
        //获取buffer中有效大小
        byte[] bs= new byte[buffer.capacity()];
        buffer.get(bs);
        return bs;
    }

    public String stringKey(ByteBuffer buffer) {
        byte[] array = bufferToByte(buffer);
        return key.decryptString(array);
    }

    public String stringValue(ByteBuffer buffer) {
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
    public String get(String key, Txn<ByteBuffer> txn) {

        ByteBuffer byteBuffer = byteKey(key);
        ByteBuffer res;
        res = db.get(txn, byteBuffer);
        if (res == null) return null;

        return stringValue(res);
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
    public <T> Map<String, T> getAsObjects(Txn<ByteBuffer> txn, Collection<String> keys) {
        Map<String, String> patch = this.getPatch(txn, keys);
        Map<String, T> res = new HashMap<>();
        patch.forEach(
                (key, value) -> {
                    Type type = new TypeToken<T>() {}.getType();
                    if (value != null) res.put(key, gson.fromJson(value, type));
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

    static class InvalidMethodException extends RuntimeException {
        public InvalidMethodException() {
            super("Lmdb: getPatch Method should not used for multiPie key-value entry,use getDuplicatedData instead");
        }
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
            System.out.println(s);
            if (s == null) {
                resList.put("?",null);
                continue;
            }
            ByteBuffer byteBuffer = byteKey(s);
            ByteBuffer res = db.get(txn, byteBuffer);
            if (res == null) resList.put(s, null);
            else {
                String value = stringValue(res);
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
        doGetPatch(txn, List.of(keys), resList);
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
    public List<Map.Entry<String, String>> prefixSearch(Txn<ByteBuffer> txn, String prefix,String pageNumber,String pageSize) {
        //TODO 测试 和 缓存（由于暂时没用到这个特性，搁置）
        ByteBuffer prefixBuffer = byteKey(prefix);
        List<Map.Entry<String, String>> res = new ArrayList<>();
        iteratePrefix(txn, prefix, prefixBuffer, res,pageSize,pageNumber);
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
                               List<Map.Entry<String, String>> res,
                               String pageSize,
                               String pageNumber) {

        int[] ints = raedPage(pageNumber, pageSize);
        int ps = ints[0];
        int pn = ints[1];
        int cur = 0;
        for (CursorIterable.KeyVal<ByteBuffer> next :
                db.iterate(txn, KeyRange.greaterThan(prefixBuffer))) {
            ByteBuffer keyBuf = next.key();
            cur++;
            if (!stringKey(keyBuf).startsWith(prefix)) break;
            if (cur > (pn-1)*ps && cur <= pn * ps){
                 res.add(MapUtil.entry(stringKey(next.key()), stringValue(next.val())));
            }
        }
    }

    /**
     * 插入一个键值对
     *
     * @param key   key String类型
     * @param value value String类型
     */
    public void put(String key, String value, Txn<ByteBuffer> txn) {
        ByteBuffer keyBuffer = byteKey(key);
        ByteBuffer valueBuffer = byteValue(value);
        Txn<ByteBuffer> txn1 = env.txn(txn);
        db.put(txn1, keyBuffer, valueBuffer);
        txn1.commit();
        txn1.close();
    }

    /**
     * 批量插入
     *
     * @param map key-value 对
     */
    public void putAll(Txn<ByteBuffer> txn, Map<String, List<String>> map) {

        try (Txn<ByteBuffer> ct = env.txn(txn)) {
            Cursor<ByteBuffer> c = db.openCursor(ct);
            for (var entry : map.entrySet()) {
                ByteBuffer keyBuffer = byteKey(entry.getKey());
                for (var a : entry.getValue()) {
                    ByteBuffer valueBuffer = byteValue(a);
                    c.put(keyBuffer, valueBuffer);
                    c.next();
                }
            }
            c.close();
            ct.commit();
        }
    }

    /**
     * 检索重复Key
     *
     * @param key String类型的Key
     * @return String类型的List 查询结果
     */
    public List<String> getDuplicatedData(String key, Txn<ByteBuffer> txn) {
        ByteBuffer keyBuffer = byteKey(key);
        List<String> resultList = new ArrayList<>();

        try (Cursor<ByteBuffer> cursor = db.openCursor(txn)) {
            cursor.get(keyBuffer,GetOp.MDB_SET_KEY);
            long count = cursor.count();
            log.info("dup found {}",count);
            String s = stringValue(cursor.val());
            resultList.add(s);

            while(cursor.seek(SeekOp.MDB_NEXT_DUP)){
                if (cursor.key().equals(keyBuffer)){
                    s = stringValue(cursor.val());
                    resultList.add(s);
                }
            }
        }

        return resultList;
    }

    private int[] raedPage(String pageNumber,String pageSize){
        int ps = 0;
        int pn = 0;
        if (Objects.equals(pageNumber, "all") || pageSize.equals("all")){
            ps = Integer.MAX_VALUE;
            pn = Integer.MAX_VALUE;
        }else{
            ps = Integer.parseInt(pageSize);
            pn = Integer.parseInt(pageNumber);
        }
        return new int[]{ps,pn};
    }



    /**
     * 检索一个范围以内的数据
     * 重复Key检索
     *
     * @param ub key的上界 可以为空，代表无上界 注意：为小于等于
     * @param lb key的下界 可以为空，代表无下届 注意：为大于等于
     * @return 符合条件的所有值
     */
    public List<Map.Entry<String, String>> getRangedDuplicatedData(Txn<ByteBuffer> parent, String lb, String ub,
                                                                   String prefix,String pageNumber,String pageSize) {

        int ps = 0;
        int pn = 0;

        int cur = 0;

        int[] ints = raedPage(pageNumber, pageSize);
        ps = ints[0];
        pn = ints[1];

        //全额返回
        Type type = new TypeToken<Map.Entry<String, String>>() {
        }.getType();

        //全额返回
        if (ub == null && lb == null) {

            List<Map.Entry<String, String>> resultList = new ArrayList<>();
            for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(parent,KeyRange.all())) {
                String s = stringValue(byteBufferKeyVal.val());
                if (!s.startsWith(prefix)) break;
                cur ++;
                if (cur > (pn-1)*ps && cur <= pn * ps){
                    resultList.add(MapUtil.entry(stringKey(byteBufferKeyVal.key()), s));
                }
            }

            return resultList;
        }

        //小于等于
        if (lb == null) {
            ByteBuffer ubBuffer = byteKey(ub);
            List<Map.Entry<String, String>> resultList = new ArrayList<>();

            for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(parent, KeyRange.closed(byteKey(prefix),ubBuffer))) {
                cur ++;
                String k = stringKey(byteBufferKeyVal.key());
                if (!k.startsWith(prefix)) break;
                if (cur > (pn-1)*ps && cur <= pn * ps){
                    String s = stringValue(byteBufferKeyVal.val());
                    resultList.add(MapUtil.entry(stringKey(byteBufferKeyVal.key()), s));
                }
            }


            return resultList;
        }

        //大于等于
        if (ub == null) {
            ByteBuffer lbBuffer = byteKey(lb);
            List<Map.Entry<String, String>> resultList = new ArrayList<>();

            for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(parent, KeyRange.atLeast(lbBuffer))) {
                cur ++;
                String k = stringKey(byteBufferKeyVal.key());
                if (!k.startsWith(prefix)) break;
                if (cur > (pn-1)*ps && cur <= pn * ps){
                    String s = stringValue(byteBufferKeyVal.val());
                    resultList.add(MapUtil.entry(stringKey(byteBufferKeyVal.key()), s));
                }

            }

            return resultList;
        }


        //上下界都有
        ByteBuffer ubBuffer = byteKey(ub);
        ByteBuffer lbBuffer = byteKey(lb);
        List<Map.Entry<String, String>> resultList = new ArrayList<>();

        String key = new String(db.getName()) + "ranged-lb-" + lb + "-ub-" + ub;

        for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(parent, KeyRange.closed(lbBuffer, ubBuffer))) {
            cur ++;
            String k = stringKey(byteBufferKeyVal.key());
            if (k.startsWith(prefix)) break;

            if (cur > (pn-1)*ps && cur <= pn * ps){
                String s = stringValue(byteBufferKeyVal.val());
                System.out.println(k);
                resultList.add(MapUtil.entry(stringKey(byteBufferKeyVal.key()), s));
            }
        }

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
            ByteBuffer byteBuffer = db.get(txn, byteKey(key));
            return stringValue(byteBuffer);
        }
    }

    /**
     * 删除重复Key中，某些value为xx的指
     *
     * @param key   String类型的Key
     * @param value String类型的value
     */
    public void deleteFromDuplicatedData(String key, String value) {
        ByteBuffer keyBuffer = byteKey(key);
        ByteBuffer valueBuffer = byteValue(value);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            CursorIterable<ByteBuffer> iterate = db.iterate(txn, KeyRange.closed(keyBuffer, keyBuffer));
            Iterator<CursorIterable.KeyVal<ByteBuffer>> iterator = iterate.iterator();
            while (iterator.hasNext()) {
                if (iterator.next().val().equals(valueBuffer)) iterator.remove();
            }
        }
    }

    /**
     * 删除重复Key中，某些value为xx的指
     *
     * @param key   String类型的Key
     * @param value String类型的value
     */
    public void deleteFromDuplicatedData(String key, String value, Txn<ByteBuffer> txn) {
        ByteBuffer keyBuffer = byteKey(key);
        ByteBuffer valueBuffer = byteValue(value);
        CursorIterable<ByteBuffer> iterate = db.iterate(txn, KeyRange.closed(keyBuffer, keyBuffer));
        Iterator<CursorIterable.KeyVal<ByteBuffer>> iterator = iterate.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().val().equals(valueBuffer)) iterator.remove();
        }
    }


    /**
     * 删除一个键值对
     *
     * @param key key String类型
     */
    public void delete(String key) {
        ByteBuffer keyBuffer = byteKey(key);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            db.delete(keyBuffer);
            txn.commit();
        }
    }


    public void deletePatch(List<String> key, Txn<ByteBuffer> txn) {
        key = new ArrayList<>(key);
        key.sort(String::compareTo);
        Set<String> keySet = new HashSet<>(key);
        String s1 = key.get(0);
        String s2 = key.get(key.size() - 1);
        ByteBuffer start = byteKey(s1);
        ByteBuffer end = byteKey(s2);
        try (Txn<ByteBuffer> sub = env.txn(txn)) {
            CursorIterable<ByteBuffer> iterate = db.iterate(sub, KeyRange.closed(start, end));
            Iterator<CursorIterable.KeyVal<ByteBuffer>> iterator = iterate.iterator();
            while (iterator.hasNext()) {
                CursorIterable.KeyVal<ByteBuffer> next = iterator.next();
                if (keySet.contains(stringKey(next.key()))) {
                    iterator.remove();
                }
            }
        }
    }

    public void deletePatch(List<String> key) {
        key.sort(String::compareTo);
        Set<String> keySet = new HashSet<>(key);
        String s1 = key.get(0);
        String s2 = key.get(key.size() - 1);
        ByteBuffer start = byteKey(s1);
        ByteBuffer end = byteKey(s2);
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            CursorIterable<ByteBuffer> iterate = db.iterate(txn, KeyRange.closed(start, end));
            Iterator<CursorIterable.KeyVal<ByteBuffer>> iterator = iterate.iterator();
            while (iterator.hasNext()) {
                CursorIterable.KeyVal<ByteBuffer> next = iterator.next();
                if (keySet.contains(stringKey(next.key()))) {
                    iterator.remove();
                }
            }
        }
    }



}


package io.dexterity.client;

import io.dexterity.client.exception.LMDBCommonException;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.lmdbjava.*;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.dexterity.client.MultipleLmdb.checkAndExpand;

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

    private final Env<ByteBuffer> env;

    public MultipleDBi(Dbi<ByteBuffer> db, String envName){
        this.db = db;
        this.env = MultipleLmdb.envs.get(envName).getEnv();
        this.envName = envName;
    }

    /**
     * String 转 ByteBuffer
     * @param s 要转的字符串
     * @return 转换结果
     */
    private ByteBuffer stringToBytes(String s){
        byte[] bytes = s.getBytes();
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        //将字符串转成ByteBuffer
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    private String bytesToString(ByteBuffer buffer){
        Charset charset = StandardCharsets.UTF_8;
        CharBuffer charBuffer = charset.decode(buffer);
        return charBuffer.toString();
    }


    public String get(String key){
        ByteBuffer byteBuffer = stringToBytes(key);
        try(Txn<ByteBuffer> txn = env.txnRead()){
            res = db.get(txn, byteBuffer);
        }
        if (res == null) return null;
        return bytesToString(res);
    }

    public Map<String,String> getPatch(String... keys){
        Map<String,String> resList = new HashMap<>();
        try(Txn<ByteBuffer> txn = env.txnRead()){
            for (var s: keys) {
                ByteBuffer byteBuffer = stringToBytes(s);
                ByteBuffer res = db.get(txn, byteBuffer);
                if (res == null) resList.put(s,null);
                else resList.put(s,bytesToString(res));
            }
        }
        if (resList.size() != keys.length)
            throw new LMDBCommonException("Lmdb patch query error:resList's length doesn't match keys");
        return resList;
    }
    /**
     * 进行前缀搜索，返回一个由key名称和keyVal对组成的集合
     * @param prefix 前缀名称
     * @return 返回一个Map
     */
    public Map<String,CursorIterable.KeyVal<ByteBuffer>> prefixSearch(String prefix){
        ByteBuffer prefixBuffer = stringToBytes(prefix);
        Map<String,CursorIterable.KeyVal<ByteBuffer>> res = new HashMap<>();
        try (Txn<ByteBuffer> txn = env.txnWrite()){
            for (CursorIterable.KeyVal<ByteBuffer> next :
                    db.iterate(txn, KeyRange.atLeast(prefixBuffer))) {
                ByteBuffer keyBuf = next.key();
                byte[] keyBytes = new byte[keyBuf.remaining()];
                keyBuf.get(keyBytes);

                String key = new String(keyBytes);
                if (!key.startsWith(prefix)) {
                    break;
                }
                res.put(key,next);
            }
        }
        return res;
    }

    /**
     * 插入一个键值对
     * @param key key String类型
     * @param value value String类型
     */
    public void put(String key,String value){
        try{
            checkAndExpand(env,key,value);
            ByteBuffer keyBuffer = stringToBytes(key);
            ByteBuffer valueBuffer = stringToBytes(value);
            db.put(keyBuffer,valueBuffer);
        }
        catch (Env.MapFullException e){
            checkAndExpand(env,key,value);
            put(key,value);
        }
    }

    /**
     * 批量插入
     * @param entryList key-value 对
     */
    public void putAll(@NotNull List<Map.Entry<String,String>> entryList){
        try(Txn<ByteBuffer> txn = env.txnWrite()){
            final Cursor<ByteBuffer> c = db.openCursor(txn);

            for (var entry: entryList) {
                ByteBuffer keyBuffer = stringToBytes(entry.getKey());
                ByteBuffer valueBuffer = stringToBytes(entry.getValue());
                c.put(keyBuffer,valueBuffer);
                c.next();
            }

            c.close();
            txn.commit();
        }catch (Env.MapFullException e){
            checkAndExpand(env,entryList);
            putAll(entryList);
        }
    }

    /**
     * 检索重复Key
     * @param key String类型的Key
     * @return String类型的List 查询结果
     */
    public List<String> getDuplicatedData(String key){
        ByteBuffer keyBuffer = stringToBytes(key);
        List<String> resultList = new ArrayList<>();
        try(Txn<ByteBuffer> txn = env.txnWrite()){
            for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn, KeyRange.closed(keyBuffer, keyBuffer))) {
                String s = bytesToString(byteBufferKeyVal.val());
                resultList.add(s);
            }
        }
        return resultList;
    }


    /**
     * 删除一个键值对
     * @param key key String类型
     */
    public void delete(String key){
        ByteBuffer keyBuffer = stringToBytes(key);
        try(Txn<ByteBuffer> txn =env.txnWrite()){
            db.delete(keyBuffer);
            txn.commit();
        }
    }

    /**
     * 删除以prefix为前缀的键值对
     * @param prefix 前缀
     */
    public void deletePrefix(String prefix){
        Map<String, CursorIterable.KeyVal<ByteBuffer>> stringKeyValMap = prefixSearch(prefix);
        try(Txn<ByteBuffer> txn = env.txnWrite()) {
            stringKeyValMap.forEach((key, value) -> db.delete(value.key()));
            txn.commit();
        }
    }

}

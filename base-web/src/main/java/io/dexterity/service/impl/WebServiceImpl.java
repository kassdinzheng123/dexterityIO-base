package io.dexterity.service.impl;

import cn.hutool.core.convert.Convert;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.dexterity.MetaDataApi;
import io.dexterity.StorageApi;
import io.dexterity.dao.WebDao;
import io.dexterity.po.vo.ChunkVO;
import io.dexterity.po.vo.RocksDBVo;
import io.dexterity.service.WebService;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Service
@Slf4j
public class WebServiceImpl extends ServiceImpl<WebDao, ChunkVO> implements WebService {
    @Autowired
    private StorageApi storageApi;
    @Autowired
    private MetaDataApi metaDataApi;
    @Autowired
    private WebDao webDao;
    @Override
    public Integer saveChunk(MultipartFile chunk, Integer index, Integer chunkTotal, Long chunkSize, String bucketName) throws RocksDBException, IOException {
        //分块信息先临时存在derby中，CHUNK_INFO表
        webDao.insert(new ChunkVO(index,chunkTotal,chunkSize,bucketName));
        //分块数据临时存在RocksDB中，chunkTmp列族
        storageApi.cfAdd("chunkTmp");
        storageApi.put(new RocksDBVo("chunkTmp", Convert.toPrimitiveByteArray(index),chunk.getBytes()));
        return 1;
    }

    public int checkChunkAll() throws RocksDBException {
        return storageApi.getAllKey("chunkTmp").size();
    }

    @Override
    public byte[] mergeChunk() throws RocksDBException {
//        List<RocksDBVo> rocksDBVos = storageApi.getAll("chunkTmp");
//        rocksDBVos.sort(Comparator.comparingInt(o -> ByteBuffer.wrap(o.getKey()).getInt()));
//        log.info("Success Sort!");
//        try {
//            ByteArrayOutputStream out = new ByteArrayOutputStream();
//            for(RocksDBVo rocksDBVo:rocksDBVos){
//                byte[] value = rocksDBVo.getValue();
//                out.write(value);
//            }
//            log.info("Success Merge!");
//            return out.toByteArray();
//        } catch (IOException e) {
//            // 异常处理
//            throw new RuntimeException("Failed to merge data", e);
//        }
        RocksIterator iterator = storageApi.getIterator("chunkTmp");
        iterator.seekToFirst();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            while (iterator.isValid()) {
                byte[] value = storageApi.get("chunkTmp",iterator.key()).getValue();
                out.write(value);
                iterator.next();
            }
            log.info("Success Merge!");
            return out.toByteArray();
        } catch (IOException e) {
            // 异常处理
            throw new RuntimeException("Failed to merge data", e);
        } finally {
            iterator.close();
        }
    }

    @Override
    public int saveObject(byte[] object,String bucketName,String fileName,String md5,Long fileSize) throws RocksDBException {
        //删除rocksdb中的临时列族,chunkTmp
        storageApi.cfDelete("chunkTmp");
        //删除derby中 的临时信息,CHUNK_INFO
        webDao.deleteChunkTemp();
        //保存对象的数据信息到rocksdb
        storageApi.put(new RocksDBVo(bucketName,fileName.getBytes(),object));
        HashMap<String,String> maps = new HashMap<>();
        maps.put("fileSize",fileSize.toString());
        maps.put("md5",md5);
        //保存对象的元数据信息到lmdb
//        metaDataApi.insertNewMetadata(new MetaData(fileName,maps),bucketName);
        return 1;
    }

    @Override
    public List<String> getAllObj(String bucketName) throws RocksDBException {
        //从rocksdb中查询
        List<byte[]> keys=storageApi.getAllKey(bucketName);
        //转换byte数组为String字符串
        return keys.stream()
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .toList();
    }

    @Override
    public int deleteObj(String bucketName, String fileName) throws RocksDBException {
        //从rocksdb中删除该对象
        storageApi.delete(bucketName,fileName.getBytes());
        return 1;
    }

    @Override
    public Boolean findObjByMD5(String md5) {
        return false;
    }

    @Override
    public List<Integer> findChunkListByMD5(String md5) {
        return new ArrayList<>();
    }
}

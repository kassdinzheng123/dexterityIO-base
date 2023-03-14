package io.dexterity.service.impl;

import cn.hutool.core.convert.Convert;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.dexterity.StorageApi;
import io.dexterity.dao.WebDao;
import io.dexterity.po.vo.ChunkVO;
import io.dexterity.po.vo.RocksDBVo;
import io.dexterity.service.WebService;
import org.rocksdb.RocksDBException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Service
public class WebServiceImpl extends ServiceImpl<WebDao, ChunkVO> implements WebService {
    @Autowired
    private StorageApi storageApi;
    @Autowired
    private WebDao webDao;
    @Override
    public Integer saveChunk(MultipartFile chunk, Integer index, Integer chunkTotal, Long chunkSize, String bucketName) throws RocksDBException, IOException {
        //分块信息先临时存在derby中
        webDao.insert(new ChunkVO(index,chunkTotal,chunkSize,bucketName));
        //分块数据临时存在RocksDB中
        storageApi.cfAdd("chunkTmp");
        storageApi.put(new RocksDBVo("chunkTmp", Convert.toPrimitiveByteArray(index),chunk.getBytes()));
        return 1;
    }

    public int checkChunkAll() throws RocksDBException {
        return storageApi.getAllKey("chunkTmp").size();
    }

    @Override
    public byte[] mergeChunk() throws RocksDBException, IOException {
        List<RocksDBVo> rocksDBVos =  storageApi.getAll("chunkTmp");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for(RocksDBVo rocksDBVo:rocksDBVos){
            byte[] value = rocksDBVo.getValue();
            out.write(value);
        }
        return out.toByteArray();
    }

    @Override
    public int saveObject(byte[] object,String bucketName,String fileName) throws RocksDBException {
        //保存对象的数据信息到rocksdb
        storageApi.put(new RocksDBVo(bucketName,fileName.getBytes(),object));
        //删除rocksdb中的临时列族,chunkTmp
        storageApi.cfDelete("chunkTmp");
        //删除derby中的临时信息,CHUNK_INFO
        webDao.deleteChunkTemp();
        //TODO 保存对象的元数据信息到lmdb

        return 1;
    }

    @Override
    public List<String>  getAllObj(String bucketName) throws RocksDBException {
        //从rocksdb中查询
        List<byte[]> keys=storageApi.getAllKey(bucketName);
        //转换byte数组为String字符串
        return keys.stream()
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .toList();
    }

    @Override
    public RocksDBVo deleteObj(String bucketName, String fileName) throws RocksDBException {
        //从rocksdb中删除该对象
        return storageApi.delete(bucketName,fileName.getBytes());
    }
}

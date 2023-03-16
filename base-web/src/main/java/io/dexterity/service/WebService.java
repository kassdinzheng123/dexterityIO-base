package io.dexterity.service;

import com.baomidou.dynamic.datasource.annotation.DSTransactional;
import com.baomidou.mybatisplus.extension.service.IService;
import io.dexterity.po.vo.ChunkVO;
import org.rocksdb.RocksDBException;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;

public interface WebService extends IService<ChunkVO> {
    Integer saveChunk(MultipartFile chunk, Integer index,
                      Integer chunkTotal, Long chunkSize,
                      String crypto,String bucketName,
                      String fileName,Long fileSize,String chunkCrypto) throws RocksDBException, IOException;

    int checkChunkAll() throws RocksDBException;

    byte[] mergeChunk() throws RocksDBException, IOException;
    @DSTransactional
    int saveObject(byte[] object,String bucketName,String fileName,String md5,Long fileSize) throws RocksDBException;

    List<String> getAllObj(String bucketName) throws RocksDBException;

    int deleteObj(String bucketName, String fileName) throws RocksDBException;

    Boolean findObjByMD5(String md5);

    List<Integer> findChunkListByMD5(String md5);
}

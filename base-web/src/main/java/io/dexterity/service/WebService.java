package io.dexterity.service;

import com.baomidou.dynamic.datasource.annotation.DSTransactional;
import com.baomidou.mybatisplus.extension.service.IService;
import io.dexterity.annotation.BucketName;
import io.dexterity.annotation.DupNames;
import io.dexterity.annotation.UnDupNames;
import io.dexterity.po.vo.ChunkVO;
import org.rocksdb.RocksDBException;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;

public interface WebService extends IService<ChunkVO> {
    Integer saveChunk(MultipartFile chunk, Integer index,
                      Integer chunkTotal, Long chunkSize,
                      String crypto, @BucketName String bucketName,
                      String fileName, Long fileSize, String chunkCrypto,
                      @DupNames List<String> dup,@UnDupNames List<String> unDup) throws RocksDBException, IOException;

    int checkChunkAll() throws RocksDBException;

    byte[] mergeChunk() throws RocksDBException, IOException;
    @DSTransactional
    int saveObject(byte[] object,String bucketName,String fileName,String md5,Long fileSize) throws RocksDBException;

    List<String> getAllObj(String bucketName) throws RocksDBException;

    int deleteObj(String bucketName, String fileName) throws RocksDBException;

    Boolean findObjByCrypto(String crypto,String bucketName);

    List<Integer> findChunkListByMD5(String md5);

    int checkChunk(String chunkCrypto, MultipartFile chunk) throws IOException;
}

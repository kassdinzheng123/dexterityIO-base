package io.dexterity.service;

import com.baomidou.mybatisplus.extension.service.IService;
import io.dexterity.po.vo.ChunkVO;
import org.rocksdb.RocksDBException;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

public interface WebService extends IService<ChunkVO> {
    Integer saveChunk(MultipartFile chunk, Integer index, Integer chunkTotal, Long chunkSize, String bucketName) throws RocksDBException, IOException;

    int checkChunkAll() throws RocksDBException;

    byte[] mergeChunk() throws RocksDBException, IOException;

    int saveObject(byte[] object,String bucketName,String fileName) throws RocksDBException;
}

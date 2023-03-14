package io.dexterity.service;

import com.baomidou.mybatisplus.extension.service.IService;
import io.dexterity.po.vo.ChunkVO;
import io.dexterity.po.vo.RocksDBVo;
import org.rocksdb.RocksDBException;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;

public interface WebService extends IService<ChunkVO> {
    Integer saveChunk(MultipartFile chunk, Integer index, Integer chunkTotal, Long chunkSize, String bucketName) throws RocksDBException, IOException;

    int checkChunkAll() throws RocksDBException;

    byte[] mergeChunk() throws RocksDBException, IOException;

    int saveObject(byte[] object,String bucketName,String fileName) throws RocksDBException;

    List<String> getAllObj(String bucketName) throws RocksDBException;

    RocksDBVo deleteObj(String bucketName, String fileName) throws RocksDBException;
}

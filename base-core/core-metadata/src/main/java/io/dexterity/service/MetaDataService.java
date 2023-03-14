package io.dexterity.service;

import io.dexterity.annotation.BucketName;
import io.dexterity.entity.MetaData;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author haoran
 * MataData的服务类，涉及到有关metadata的各种对外暴露的接口
 */
@Service
public interface MetaDataService {
    void insertNewMetadata(MetaData metaData,String bucketKey);
    void deleteMetadata(List<String> metadataKey,String bucketKey);
    Set<String> selectByMetaData(MetaData metaData, String bucketKey);
    void addNewMetadata(MetaData matcher,String bucketKey,String newMdKey,String newMdValue);
    void insertPatch(List<MetaData> metaData, @BucketName String bucketKey);
    Map<String,MetaData> selectMdByKeys(List<String> key, String bucketKey);
    Map<String,MetaData> selectMdByMdRange(String metadataKey, String lb, String ub, String bucketKey);
    Map<String,MetaData> selectMdByKeyRange(String lb, String ub, String bucketKey);
    Map<String,MetaData> selectMdByKeyPrefix(String prefix,String bucketKey);


}

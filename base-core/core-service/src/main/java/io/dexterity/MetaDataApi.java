package io.dexterity;

import io.dexterity.annotation.BucketName;
import io.dexterity.entity.MetaData;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MetaDataApi {
    void insertNewMetadata(MetaData metaData, String bucketKey);
    void deleteMetadata(String metadataKey,String bucketKey);
    Set<String> selectByMetaData(MetaData metaData, String bucketKey);
    void addNewMetadata(MetaData matcher,String bucketKey,String newMdKey,String newMdValue);
    void insertPatch(List<MetaData> metaData, @BucketName String bucketKey);
    Map<String,MetaData> selectMetadata(List<String> key, String bucketKey);
}

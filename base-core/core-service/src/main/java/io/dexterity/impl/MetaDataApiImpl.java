package io.dexterity.impl;

import io.dexterity.MetaDataApi;
import io.dexterity.entity.MetaData;
import io.dexterity.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class MetaDataApiImpl implements MetaDataApi {
    @Autowired
    private MetaDataService metaDataService;


    @Override
    public void insertNewMetadata(MetaData metaData, String bucketKey) {
        metaDataService.insertNewMetadata(metaData,bucketKey);
    }

    @Override
    public void deleteMetadata(String metadataKey, String bucketKey) {
        metaDataService.deleteMetadata(metadataKey,bucketKey);
    }

    @Override
    public Set<String> selectByMetaData(MetaData metaData, String bucketKey) {
        return metaDataService.selectByMetaData(metaData,bucketKey);
    }

    @Override
    public void addNewMetadata(MetaData matcher, String bucketKey, String newMdKey, String newMdValue) {
        metaDataService.addNewMetadata(matcher,bucketKey,newMdKey,newMdValue);
    }

    @Override
    public void insertPatch(List<MetaData> metaData, String bucketKey) {
        metaDataService.insertPatch(metaData,bucketKey);
    }

    @Override
    public Map<String, MetaData> selectMetadata(List<String> key, String bucketKey) {
        return metaDataService.selectMetadata(key,bucketKey);
    }
}

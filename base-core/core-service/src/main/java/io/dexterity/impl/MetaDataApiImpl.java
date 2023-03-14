package io.dexterity.impl;

import io.dexterity.MetaDataApi;
import io.dexterity.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MetaDataApiImpl implements MetaDataApi {
    @Autowired
    private MetaDataService metaDataService;

}

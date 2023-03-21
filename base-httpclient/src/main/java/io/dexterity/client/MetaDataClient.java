package io.dexterity.client;


import io.dexterity.entity.BucketInfo;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.annotation.PutExchange;

@HttpExchange("/metadata")
public interface MetaDataClient {

    @PutExchange("/open")
    public void openEnv(BucketInfo bucketInfo);

}

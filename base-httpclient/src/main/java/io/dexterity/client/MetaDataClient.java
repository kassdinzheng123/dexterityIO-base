package io.dexterity.client;


import io.dexterity.entity.MetaData;
import io.dexterity.po.R;
import org.lmdbjava.Txn;
import org.springframework.web.service.annotation.HttpExchange;

import java.nio.ByteBuffer;

@HttpExchange("/metadata")
public interface MetaDataClient {
    R<?> insertNewMetadata(MetaData metaData, MultipleEnv env, Txn<ByteBuffer> parent);
}

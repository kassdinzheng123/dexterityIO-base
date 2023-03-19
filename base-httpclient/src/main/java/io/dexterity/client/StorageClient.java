package io.dexterity.client;

import io.dexterity.po.R;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;

@HttpExchange("/storage")
public interface StorageClient {
    @GetExchange
    R<?> cfAll();
}

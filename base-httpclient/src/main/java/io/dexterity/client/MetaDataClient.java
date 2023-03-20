package io.dexterity.client;


import io.dexterity.entity.exchange.ChunkDTO;
import io.dexterity.entity.exchange.RangeQuery;
import io.dexterity.po.R;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.service.annotation.DeleteExchange;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.annotation.PostExchange;

import java.util.List;

@HttpExchange("/metadata")
public interface MetaDataClient {

    @PostExchange("/object")
     R<?> uploadToBucket(
            @RequestParam("chunk") MultipartFile chunk, //块的数据
            @RequestParam("param") ChunkDTO param);

    @PostExchange("/list")
     R<?> listObjects(@RequestBody RangeQuery rangeQuery);

    @GetExchange("/{key}")
    R<?> getObject(@PathVariable String key);

    @DeleteExchange("/{key}")
    R<?> deleteObject(@PathVariable String key);

    @DeleteExchange("/?delete")
     R<?> deleteObjects(@RequestBody List<String> objectIds);

}

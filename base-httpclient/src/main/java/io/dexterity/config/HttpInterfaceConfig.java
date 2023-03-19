package io.dexterity.config;

import io.dexterity.client.MetaDataClient;
import io.dexterity.client.StorageClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.support.WebClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

@Configuration
public class HttpInterfaceConfig {

    @Bean
    MetaDataClient metaDataClient() {
        WebClient client = WebClient.builder().baseUrl("http://localhost:9091").build();
        HttpServiceProxyFactory factory = HttpServiceProxyFactory.builder(WebClientAdapter.forClient(client)).build();
        return factory.createClient(MetaDataClient.class);
    }

    @Bean
    StorageClient storageClient() {
        WebClient client = WebClient.builder().baseUrl("http://localhost:9092").build();
        HttpServiceProxyFactory factory = HttpServiceProxyFactory.builder(WebClientAdapter.forClient(client)).build();
        return factory.createClient(StorageClient.class);
    }
}
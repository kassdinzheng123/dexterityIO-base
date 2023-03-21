package io.dexterity.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("metadata")
@Data
public class LmdbConfigProperties {
    private String bucketDataRoot;
    private String mainDBRoot;

    private long initSize = 1024*1024*10;

}

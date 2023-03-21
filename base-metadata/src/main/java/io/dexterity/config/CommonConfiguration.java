package io.dexterity.config;

import io.dexterity.config.properties.LmdbConfigProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({LmdbConfigProperties.class})
public class CommonConfiguration {
}

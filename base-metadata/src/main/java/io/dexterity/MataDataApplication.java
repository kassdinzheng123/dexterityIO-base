package io.dexterity;

import io.dexterity.config.properties.LmdbConfigProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({LmdbConfigProperties.class})
public class MataDataApplication {
    public static void main(String[] args) {
        SpringApplication.run(MataDataApplication.class);
    }
}

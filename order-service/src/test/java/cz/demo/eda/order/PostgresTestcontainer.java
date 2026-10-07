package cz.demo.eda.order;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** PostgreSQL pro testy – stejná major verze jako v Kubernetes. */
@TestConfiguration(proxyBeanMethods = false)
public class PostgresTestcontainer {

    public static final String IMAGE = "postgres:18.6-alpine";

    /** Kontejner sdílený v rámci Spring test kontextu; @ServiceConnection nastaví datasource. */
    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer(IMAGE);
    }
}

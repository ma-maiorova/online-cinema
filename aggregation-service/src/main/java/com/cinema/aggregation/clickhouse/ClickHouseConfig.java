package com.cinema.aggregation.clickhouse;

import com.clickhouse.jdbc.ClickHouseDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Конфигурация DataSource для ClickHouse.
 *
 * <p>Использует ClickHouse JDBC драйвер напрямую, отдельно от основного
 * Spring DataSource (PostgreSQL).
 */
@Slf4j
@Configuration
public class ClickHouseConfig {

    @Value("${cinema.clickhouse.url}")
    private String url;

    @Value("${cinema.clickhouse.user}")
    private String user;

    @Value("${cinema.clickhouse.password}")
    private String password;

    @Bean(name = "clickHouseDataSource")
    public DataSource clickHouseDataSource() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("socket_timeout", "60000");
        props.setProperty("connection_timeout", "10000");

        log.info("[CLICKHOUSE_CONFIG] Подключение к ClickHouse: {}", url);
        return new ClickHouseDataSource(url, props);
    }
}

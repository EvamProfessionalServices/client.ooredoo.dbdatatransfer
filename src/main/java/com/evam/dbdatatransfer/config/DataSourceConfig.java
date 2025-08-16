package com.evam.dbdatatransfer.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    private final AppProperties appProperties;

    public DataSourceConfig(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

    @Bean(name = "sourceDataSource")
    public DataSource sourceDataSource() {
        AppProperties.Database dbProps = appProperties.getSourceDb();
        return DataSourceBuilder.create()
                .url(dbProps.getUrl())
                .username(dbProps.getUsername())
                .password(dbProps.getPassword())
                .driverClassName(dbProps.getDriverClassName())
                .build();
    }

    @Bean(name = "targetDataSource")
    public DataSource targetDataSource() {
        AppProperties.Database dbProps = appProperties.getTargetDb();
        return DataSourceBuilder.create()
                .url(dbProps.getUrl())
                .username(dbProps.getUsername())
                .password(dbProps.getPassword())
                .driverClassName(dbProps.getDriverClassName())
                .build();
    }

    @Bean(name = "sourceJdbcTemplate")
    public JdbcTemplate sourceJdbcTemplate(@Qualifier("sourceDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "targetJdbcTemplate")
    public JdbcTemplate targetJdbcTemplate(@Qualifier("targetDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
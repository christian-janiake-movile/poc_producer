package com.movile.pocproducer;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
@ComponentScan("com.movile.pocproducer")
@PropertySource("application.properties")
public class SystemResources {

    @Autowired
    Logger log;

    @Autowired
    Environment env;

    @Bean
    DataSource databaseDatasource() {
        ComboPooledDataSource datasource = new ComboPooledDataSource();
        String driver = env.getProperty("database.driver");
        String url = env.getProperty("database.url");
        String user = env.getProperty("database.user");
        String password = env.getProperty("database.password");

        try {
            datasource.setDriverClass(driver);
            datasource.setJdbcUrl(url);
            datasource.setUser(user);
            datasource.setPassword(password);

        } catch(Exception e) {
            log.error("Error connecting database", e);
            throw new RuntimeException(e);
        }
        return datasource;
    }

    @Bean
    JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}

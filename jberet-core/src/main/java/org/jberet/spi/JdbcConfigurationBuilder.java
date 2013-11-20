/*
 * Copyright (c) 2013 Red Hat, Inc. and/or its affiliates.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.jberet.spi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.jberet._private.BatchMessages;

/**
 * @author <a href="mailto:jperkins@redhat.com">James R. Perkins</a>
 */
// TODO (jrp) possibly make this abstract
public class JdbcConfigurationBuilder {
    // TODO (jrp) remove below here
    //keys used in jberet.properties
    public static final String DDL_FILE_NAME_KEY = "ddl-file";
    public static final String SQL_FILE_NAME_KEY = "sql-file";
    public static final String DATASOURCE_JNDI_KEY = "datasource-jndi";
    public static final String DB_URL_KEY = "db-url";
    public static final String DB_USER_KEY = "db-user";
    public static final String DB_PASSWORD_KEY = "db-password";
    public static final String DB_PROPERTIES_KEY = "db-properties";
    public static final String DB_PROPERTY_DELIM = ":";

    //defaults for entries in jberet.properties
    private static final String DEFAULT_DB_URL = "jdbc:h2:~/jberet-repo";
    private static final String DEFAULT_SQL_FILE = "sql/jberet-h2-sql.properties";
    private static final String DEFAULT_DDL_FILE = "sql/jberet-h2.ddl";
    // TODO (jrp) remove above here

    // TODO (jrp) better names
    private String ddlFileName = null;
    private String sqlFileName = null;
    private String dbUrl = null;
    private DataSource dataSource = null;
    private String datasourceJndi;
    private final Properties dbProperties;

    public JdbcConfigurationBuilder() {
        dbProperties = new Properties();
    }

    public static JdbcConfigurationBuilder create() {
        return new JdbcConfigurationBuilder();
    }

    public static JdbcConfigurationBuilder from(final Properties configProperties) {
        JdbcConfigurationBuilder result = new JdbcConfigurationBuilder()
                .setDataSourceName(configProperties.getProperty(DATASOURCE_JNDI_KEY))
                .setConnectionUrl(configProperties.getProperty(DB_URL_KEY))
                .setDdlFileName(configProperties.getProperty(DDL_FILE_NAME_KEY))
                .setSqlFileName(configProperties.getProperty(SQL_FILE_NAME_KEY))
                .setUsername(configProperties.getProperty(DB_USER_KEY))
                .setPassword(configProperties.getProperty(DB_PASSWORD_KEY));
        final String s = configProperties.getProperty(DB_PROPERTIES_KEY);
        if (s != null) {
            final String[] ss = s.split(DB_PROPERTY_DELIM);
            for (final String kv : ss) {
                final int equalSign = kv.indexOf('=');
                if (equalSign > 0) {
                    result.setConnectionProperty(kv.substring(0, equalSign), kv.substring(equalSign + 1));
                }
            }
        }
        return result;
    }

    public JdbcConfigurationBuilder setConnectionProperty(final String key, final String value) {
        // TODO (jrp) should value be allowed to be null?
        if (key != null)
            dbProperties.setProperty(key, value);
        return this;
    }

    public JdbcConfigurationBuilder setConnectionUrl(final String url) {
        this.dbUrl = url;
        return this;
    }

    public JdbcConfigurationBuilder setUsername(final String username) {
        if (username != null)
            dbProperties.setProperty("user", username);
        return this;
    }

    public JdbcConfigurationBuilder setPassword(final String password) {
        if (password != null)
            dbProperties.setProperty("password", password);
        return this;
    }

    public JdbcConfigurationBuilder setDdlFileName(final String ddlFileName) {
        this.ddlFileName = ddlFileName;
        return this;
    }

    public JdbcConfigurationBuilder setSqlFileName(final String sqlFileName) {
        this.sqlFileName = sqlFileName;
        return this;
    }

    public JdbcConfigurationBuilder setDataSourceName(final String dataSourceName) {
        if (dataSource != null) {
            // TODO (jrp) i18n
            throw new IllegalStateException("DataSource already set");
        }
        this.datasourceJndi = dataSourceName;
        return this;
    }

    public JdbcConfigurationBuilder setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public JdbcConfiguration build() {
        final DataSource dataSource;
        if (this.dataSource != null) {
            dataSource = this.dataSource;
        } else if (datasourceJndi != null) {
            try {
                dataSource = InitialContext.doLookup(datasourceJndi);
            } catch (NamingException e) {
                throw BatchMessages.MESSAGES.failToLookupDataSource(e, datasourceJndi);
            }
        } else {
            dataSource = null;
        }
        final String dbUrl = which(this.dbUrl, DEFAULT_DB_URL);
        final String ddlFileName = which(this.ddlFileName, DEFAULT_DDL_FILE);
        final String sqlFileName = which(this.sqlFileName, DEFAULT_SQL_FILE);
        return new JdbcConfiguration(ddlFileName, sqlFileName) {
            @Override
            public Connection getConnection() {
                if (dataSource != null) {
                    try {
                        return dataSource.getConnection();
                    } catch (SQLException e) {
                        // TODO (jrp) verify this
                        throw BatchMessages.MESSAGES.failToObtainConnection(e, dataSource, datasourceJndi);
                    }
                } else {
                    try {
                        return DriverManager.getConnection(dbUrl, dbProperties);
                    } catch (Exception e) {
                        throw BatchMessages.MESSAGES.failToObtainConnection(e, dbUrl, dbProperties);
                    }
                }
            }
        };
    }

    private static <T> T which(final T value, final T defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return value;
    }
}

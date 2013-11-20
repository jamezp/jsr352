/*
 * Copyright (c) 2013 Red Hat, Inc. and/or its affiliates.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 * Cheng Fang - Initial API and implementation
 */

package org.jberet.se;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;

import org.jberet.se._private.SEBatchLogger;
import org.jberet.spi.ArtifactFactory;
import org.jberet.spi.BatchEnvironment;
import org.jberet.spi.Configuration;
import org.jberet.spi.Configuration.RepositoryType;
import org.jberet.spi.JdbcConfigurationBuilder;

/**
 * Represents the Java SE batch runtime environment and its services.
 */
public final class BatchSEEnvironment implements BatchEnvironment {
    //keys used in jberet.properties
    public static final String JOB_REPOSITORY_TYPE_KEY = "job-repository-type";
    public static final String DDL_FILE_NAME_KEY = "ddl-file";
    public static final String SQL_FILE_NAME_KEY = "sql-file";
    public static final String DATASOURCE_JNDI_KEY = "datasource-jndi";
    public static final String DB_URL_KEY = "db-url";
    public static final String DB_USER_KEY = "db-user";
    public static final String DB_PASSWORD_KEY = "db-password";
    public static final String DB_PROPERTIES_KEY = "db-properties";
    public static final String DB_PROPERTY_DELIM = ":";

    private static final ExecutorService executorService = Executors.newCachedThreadPool(new BatchThreadFactory());

    public static final String CONFIG_FILE_NAME = "jberet.properties";

    private volatile Configuration configuration;

    private final UserTransaction ut;

    public BatchSEEnvironment() {
        this.ut = com.arjuna.ats.jta.UserTransaction.userTransaction();
    }

    @Override
    public ClassLoader getClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = BatchSEEnvironment.class.getClassLoader();
        }
        return cl;
    }

    @Override
    public ArtifactFactory getArtifactFactory() {
        return new SEArtifactFactory();
    }

    @Override
    public Future<?> submitTask(final Runnable task) {
        return executorService.submit(task);
    }

    @Override
    public <T> Future<T> submitTask(final Runnable task, final T result) {
        return executorService.submit(task, result);
    }

    @Override
    public <T> Future<T> submitTask(final Callable<T> task) {
        return executorService.submit(task);
    }

    @Override
    public UserTransaction getUserTransaction() {
        return this.ut;
    }

    @Override
    public Configuration getBatchConfiguration() {
        Configuration result = configuration;
        if (result == null) {
            synchronized (this) {
                result = configuration;
                if (result == null) {
                    final Properties configProperties = new Properties();
                    final InputStream configStream = getClassLoader().getResourceAsStream(CONFIG_FILE_NAME);
                    if (configStream != null) {
                        try {
                            configProperties.load(configStream);
                        } catch (IOException e) {
                            throw SEBatchLogger.LOGGER.failToLoadConfig(e, CONFIG_FILE_NAME);
                        }
                    } else {
                        SEBatchLogger.LOGGER.useDefaultJBeretConfig(CONFIG_FILE_NAME);
                    }
                    result = configuration = Configuration.Factory.from(configProperties);
                }
            }
        }
        return result;
    }
}

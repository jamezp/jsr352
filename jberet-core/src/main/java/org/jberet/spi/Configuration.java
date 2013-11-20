/*
 * Copyright (c) 2013 Red Hat, Inc. and/or its affiliates.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.jberet.spi;

import java.util.Properties;

import org.jberet._private.BatchMessages;

/**
 * @author <a href="mailto:jperkins@redhat.com">James R. Perkins</a>
 */
// TODO (jrp) abstract class instead?
public interface Configuration {

    public Configuration IN_MEMORY = new Configuration() {
        @Override
        public RepositoryType getRepositoryType() {
            return RepositoryType.IN_MEMORY;
        }
    };

    public enum RepositoryType {
        IN_MEMORY {
            @Override
            public String toString() {
                return "in-memory";
            }
        },
        JDBC {
            @Override
            public String toString() {
                return "jdbc";
            }
        };

        public static RepositoryType of(final String repositoryType) {
            if ("jdbc".equalsIgnoreCase(repositoryType)) {
                return JDBC;
            }
            if ("in-memory".equalsIgnoreCase(repositoryType) || "in_memory".equalsIgnoreCase(repositoryType)) {
                return IN_MEMORY;
            }
            throw BatchMessages.MESSAGES.unrecognizedJobRepositoryType(repositoryType);
        }
    }

    final String JOB_REPOSITORY_TYPE_KEY = "job-repository-type";

    RepositoryType getRepositoryType();

    public class Factory {

        public static Configuration from(final Properties configProperties) {
            final String repositoryType = configProperties.getProperty(JOB_REPOSITORY_TYPE_KEY);
            if (repositoryType != null && RepositoryType.of(repositoryType) == RepositoryType.IN_MEMORY) {
                return IN_MEMORY;
            }
            return JdbcConfigurationBuilder.from(configProperties).build();
        }
    }
}

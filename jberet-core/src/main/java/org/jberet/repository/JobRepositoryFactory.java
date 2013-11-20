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

package org.jberet.repository;

import java.util.Properties;

import org.jberet._private.BatchMessages;
import org.jberet.spi.BatchEnvironment;
import org.jberet.spi.Configuration;
import org.jberet.spi.Configuration.RepositoryType;

public final class JobRepositoryFactory {

    private JobRepositoryFactory() {
    }

    public static JobRepository getJobRepository(final BatchEnvironment batchEnvironment) {
        RepositoryType repositoryType = null;
        if (batchEnvironment != null) {
            repositoryType = batchEnvironment.getBatchConfiguration().getRepositoryType();
        }
        if (repositoryType == null || repositoryType == RepositoryType.JDBC) {
            return JdbcRepository.getInstance(batchEnvironment);
        } else if (repositoryType == RepositoryType.IN_MEMORY) {
            return InMemoryRepository.getInstance(batchEnvironment);
        } else {
            throw BatchMessages.MESSAGES.unrecognizedJobRepositoryType(repositoryType.toString());
        }
    }
}

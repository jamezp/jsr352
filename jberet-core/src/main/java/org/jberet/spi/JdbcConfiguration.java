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

/**
 * @author <a href="mailto:jperkins@redhat.com">James R. Perkins</a>
 */
public abstract class JdbcConfiguration implements Configuration {
    private final String ddlFileName;
    private final String sqlFileName;

    protected JdbcConfiguration(final String ddlFileName, final String sqlFileName) {
        this.ddlFileName = ddlFileName;
        this.sqlFileName = sqlFileName;
    }

    @Override
    public RepositoryType getRepositoryType() {
        return RepositoryType.JDBC;
    }

    public abstract Connection getConnection();


    public String getDdlFileName() {
        return ddlFileName;
    }

    public String getSqlFileName() {
        return sqlFileName;
    }
}

/*
 * Copyright (c) 2016 Red Hat, Inc. and/or its affiliates.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.jberet.spi;

import org.jberet.operations.JobOperatorImpl;

/**
 * @author <a href="mailto:jperkins@redhat.com">James R. Perkins</a>
 */
public class DefaultJobOperatorContextSelector implements JobOperatorContextSelector {
    private final JobOperatorContext jobOperatorContext;

    public DefaultJobOperatorContextSelector() {
        jobOperatorContext = JobOperatorContext.create(new JobOperatorImpl());
    }

    public DefaultJobOperatorContextSelector(final BatchEnvironment batchEnvironment) {
        jobOperatorContext = JobOperatorContext.create(batchEnvironment);
    }

    @Override
    public JobOperatorContext getJobOperatorContext() {
        return jobOperatorContext;
    }
}

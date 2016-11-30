/*
 * Copyright (c) 2016 Red Hat, Inc. and/or its affiliates.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.jberet.spi;

import javax.batch.operations.JobOperator;

import org.jberet.operations.JobOperatorImpl;
import org.jberet.util.Assertions;

/**
 * A context on which the {@link JobOperator} can be found.
 *
 * @author <a href="mailto:jperkins@redhat.com">James R. Perkins</a>
 */
@SuppressWarnings("unused")
public abstract class JobOperatorContext {

    private static volatile JobOperatorContextSelector SELECTOR = null;

    private static class DefaultHolder {
        // Used to lazy-load, in some cases there may not be a BatchEnvironment service implementation
        static final JobOperatorContextSelector DEFAULT = new DefaultJobOperatorContextSelector();
    }

    /**
     * Returns the current context based on the selector.
     *
     * @return the current context
     */
    public static JobOperatorContext getJobOperatorContext() {
        JobOperatorContextSelector selector = SELECTOR;
        if (selector == null) {
            selector = DefaultHolder.DEFAULT;
        }
        return selector.getJobOperatorContext();
    }

    /**
     * Creates a new context based on the environment.
     *
     * @param batchEnvironment the batch environment to create the context for, cannot be {@code null}
     *
     * @return the new context
     */
    public static JobOperatorContext create(final BatchEnvironment batchEnvironment) {
        final JobOperator jobOperator = new JobOperatorImpl(Assertions.notNull(batchEnvironment, "batchEnvironment"));
        return new JobOperatorContext() {
            @Override
            public JobOperator getJobOperator() {
                return jobOperator;
            }
        };
    }

    /**
     * Creates a new context which returns the job operator.
     *
     * @param jobOperator the job operator this context should return, cannot be {@code null}
     *
     * @return the new context
     */
    public static JobOperatorContext create(final JobOperator jobOperator) {
        Assertions.notNull(jobOperator, "jobOperator");
        return new JobOperatorContext() {
            @Override
            public JobOperator getJobOperator() {
                return jobOperator;
            }
        };
    }

    /**
     * Allows the selector for the {@link JobOperatorContext} to be set. The parameter cannot be {@code null}.
     *
     * @param selector the selector to use
     */
    public static void setJobOperatorContextSelector(final JobOperatorContextSelector selector) {
        SELECTOR = Assertions.notNull(selector, "selector");
    }

    /**
     * Returns the {@link JobOperator} for this current context.
     *
     * @return the job operator
     */
    public abstract JobOperator getJobOperator();
}

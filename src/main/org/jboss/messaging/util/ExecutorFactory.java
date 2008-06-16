/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.util;

import java.util.concurrent.Executor;

/**
 * 
 * A ExecutorFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ExecutorFactory
{
   Executor getExecutor();
}

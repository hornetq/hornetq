/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf.framework.persistence;

import org.jboss.test.messaging.jms.perf.framework.data.Benchmark;
import org.jboss.test.messaging.jms.perf.framework.data.Execution;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface PersistenceManager
{
   Benchmark getBenchmark(String name);
   
   void saveExecution(Execution exec);
   
   void deleteAllResults();
   
   void start();
   
   void stop();
   
}

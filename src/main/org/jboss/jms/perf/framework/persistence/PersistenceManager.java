/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.persistence;

import org.jboss.jms.perf.framework.data.Benchmark;
import org.jboss.jms.perf.framework.data.Execution;

/**
 * 
 * A PersistenceManager.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public interface PersistenceManager
{
   Benchmark getBenchmark(String name);
   
   void saveExecution(Execution exec);
   
   void deleteAllResults();
   
   void start();
   
   void stop();
   
}

/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.persistence;

import org.jboss.jms.perf.framework.data.PerformanceTest;

import java.util.List;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public interface PersistenceManager
{
   List getPerformanceTestNames() throws Exception;

   PerformanceTest getPerformanceTest(String name) throws Exception;

   void savePerformanceTest(PerformanceTest test) throws Exception;

   void deleteAllResults() throws Exception;

   void start() throws Exception;

   void stop() throws Exception;

}

/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import java.util.Map;

public interface Processor
{
   void processResults(Test test, Configuration config, Map results, ResultPersistor persistor);
}

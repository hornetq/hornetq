/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf.framework;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface Servitor extends Runnable
{ 
   boolean isFailed();
   
   Throwable getThrowable();
   
   void init();
   
   void deInit(); 
}

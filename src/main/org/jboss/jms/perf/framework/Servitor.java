/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

/**
 * 
 * A Servitor.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public interface Servitor extends Runnable
{ 
   boolean isFailed();
   
   Throwable getThrowable();
   
   void init();
   
   void deInit(); 
}

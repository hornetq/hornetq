/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.protocol;

/**
 * 
 * A Servitor.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
interface Servitor extends Runnable
{ 
   boolean isFailed();
   
   void init();
   
   void deInit();
   
   long getTime();
   
   int getMessages();
}

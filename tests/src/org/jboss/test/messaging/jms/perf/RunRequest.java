/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RunRequest implements ServerRequest
{
   private static final long serialVersionUID = 5354671724291558230L;
   
   Job job;
   
   public RunRequest(Job test)
   {
      this.job = test;
   }
   
   public Object execute(JobStore store)
   {
      store.addJob(job);
      new Thread(job).start();
      return null;
   }
}

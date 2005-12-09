/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class RunRequest implements ServerRequest
{
   private static final long serialVersionUID = 5354671724291558230L;
   
   Job job;
   
   public RunRequest(Job test)
   {
      this.job = test;
   }
   
   public JobResult execute()
   {
      job.run();      
      return job.getResult();
   }
}

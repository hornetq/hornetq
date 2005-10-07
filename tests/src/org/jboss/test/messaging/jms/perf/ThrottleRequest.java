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
public class ThrottleRequest implements ServerRequest
{
     
   String jobName;
   
   double tp;
   
   public ThrottleRequest(String jobName, double tp)
   {
      this.jobName = jobName;
      this.tp = tp;
   }
   
   public Object execute(JobStore store)
   {
      SenderJob job = (SenderJob)store.getJob(jobName);
      job.throttle(tp);
      return null;
   }
}

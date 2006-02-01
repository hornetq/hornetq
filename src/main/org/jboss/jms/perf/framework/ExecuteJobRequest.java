/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

/**
 * 
 * A ExecuteJobRequest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * ExecuteJobRequest.java,v 1.1 2006/02/01 17:38:30 timfox Exp
 */
public class ExecuteJobRequest implements ServerRequest
{
   private static final long serialVersionUID = 5354671724291558230L;
   
   String jobId;
   
   public ExecuteJobRequest(String jobId)
   {
      this.jobId = jobId;
   }
   
   public Object execute(JobStore store) throws Exception
   {
      Job job = store.getJob(jobId);
      
      if (job == null)
      {
         throw new IllegalStateException("Cannot find job with id:" + jobId);
      }
      
      JobResult res = job.execute();
      
      return res;
   }
}

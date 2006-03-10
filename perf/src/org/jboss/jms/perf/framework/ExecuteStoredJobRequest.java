/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version 1.1
 *
 * ExecuteStoredJobRequest.java,v 1.1 2006/02/01 17:38:30 timfox Exp
 */
public class ExecuteStoredJobRequest implements ServerRequest
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 5354671724291558230L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String jobID;

   // Constructors --------------------------------------------------

   public ExecuteStoredJobRequest(String jobID)
   {
      this.jobID = jobID;
   }

   // ServerRequest implementation ----------------------------------

   public Object execute(JobStore store) throws Exception
   {
      Job job = store.getJob(jobID);

      if (job == null)
      {
         throw new IllegalStateException("Cannot find job with id " + jobID);
      }

      try
      {
         return job.execute();
      }
      finally
      {
         store.removeJob(jobID);
      }
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "ExecuteStoredJobRequest[" + jobID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------



}

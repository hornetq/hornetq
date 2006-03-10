/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.data;

import org.jboss.logging.Logger;
import org.jboss.jms.perf.framework.Job;
import org.jboss.jms.perf.framework.ThroughputResult;
import org.jboss.jms.perf.framework.StoreJobRequest;
import org.jboss.jms.perf.framework.ExecuteStoredJobRequest;
import org.jboss.jms.perf.framework.Runner;
import org.jboss.jms.perf.framework.BaseJob;
import org.jboss.jms.perf.framework.Failure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Properties;

/**
 * 
 * A PerformanceTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class PerformanceTest implements Serializable, JobList
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 4821514879181362348L;

   private static final Logger log = Logger.getLogger(PerformanceTest.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private Runner runner;

   protected long id;
   protected String name;
   protected List executions;

   protected String destination;
   protected String connectionFactory;
   protected List jobs;

   // Constructors --------------------------------------------------

   public PerformanceTest(Runner runner, String name)
   {
      this.runner = runner;
      this.name = name;
      id = Long.MIN_VALUE;
      executions = new ArrayList();
      jobs = new ArrayList();
   }

   // JobList implementation ----------------------------------------

   public void addJob(Job job)
   {
      jobs.add(job);
   }

   public int size()
   {
      return jobs.size();
   }

   public Iterator iterator()
   {
      return jobs.iterator();
   }

   // Public --------------------------------------------------------

   public void addParallelJobs(JobList parallelJobs)
   {
      jobs.add(parallelJobs);
   }

   public void setDestination(String destination)
   {
      this.destination = destination;
   }

   public String getDestination()
   {
      return destination;
   }

   public void setConnectionFactory(String cf)
   {
      this.connectionFactory = cf;
   }

   public String getConnectionFactory()
   {
      return connectionFactory;
   }

   public void addExecution(Execution exec)
   {
      executions.add(exec);
   }

   public List getExecutions()
   {
      return executions;
   }

   public String getName()
   {
      return name;
   }

   public void setName(String name)
   {
      this.name = name;
   }

   public long getId()
   {
      return id;
   }

   public void run() throws Exception
   {
      log.info("");
      log.info("Performance Test \"" + getName() + "\"");

      int executionCounter = 1;

      for(Iterator ei = executions.iterator(); ei.hasNext(); )
      {
         Execution e = (Execution)ei.next();

         String provider = e.getProviderName();
         Properties providerJNDIProperties = runner.getConfiguration().getJNDIProperties(provider);

         log.info("");
         log.info("Execution " + executionCounter++ + " (provider " + provider + ")");

         for(Iterator i = jobs.iterator(); i.hasNext(); )
         {
            Object o = i.next();

            if (o instanceof Job)
            {
               Job job = (Job)o;
               configureJob(job, providerJNDIProperties);

               ThroughputResult result = runJob(job);

               log.info(e.size() + " " + result);

               e.addMeasurement(result);
            }
            else
            {
               JobList parallelJobs = (JobList)o;
               configureJobs(parallelJobs, providerJNDIProperties);

               List results = runParallelJobs(parallelJobs);

               log.info(e.size() + " PARALLEL");

               for(Iterator ri = results.iterator(); ri.hasNext(); )
               {
                  ThroughputResult r = (ThroughputResult)ri.next();
                  log.info("    " + r);
               }

               e.addMeasurement(results);
            }
         }
      }
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer();

      sb.append("PerformanceTest[").append(name).append("](");
      for(Iterator i = executions.iterator(); i.hasNext(); )
      {
         Execution e = (Execution)i.next();
         sb.append(e.getProviderName());
         if (i.hasNext())
         {
            sb.append(", ");
         }
      }
      sb.append(")");

      return sb.toString();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void configureJobs(JobList jobList, Properties providerJNDIProperties)
   {
      for(Iterator i = jobList.iterator(); i.hasNext(); )
      {
         Job j = (Job)i.next();
         configureJob(j, providerJNDIProperties);
      }
   }

   private void configureJob(Job job, Properties providerJNDIProperties)
   {
      // apply defaults, in case values are not set

      BaseJob baseJob = (BaseJob)job;

      if (baseJob.getDestinationName() == null)
      {
         baseJob.setDestinationName(destination);
      }

      if (baseJob.getConnectionFactoryName() == null)
      {
         baseJob.setConnectionFactoryName(connectionFactory);
      }

      baseJob.setJNDIProperties(providerJNDIProperties);
   }

   private boolean local = false;

   private ThroughputResult runJob(Job job) throws Exception
   {
      ThroughputResult result = null;

      try
      {
      if (local)
      {
         job.initialize();
         result = job.execute();
      }
      else
      {
         String executorURL = job.getExecutorURL();

         if (executorURL == null)
         {
            throw new Exception("At least on executorURL must be configured");
         }

         if (log.isTraceEnabled()) { log.trace("submitting job " + job + " to " + executorURL); }
         Runner.sendRequestToExecutor(executorURL, new StoreJobRequest(job));


         if (log.isTraceEnabled()) { log.trace("executing job " + job + " on " + executorURL); }
         result = Runner.sendRequestToExecutor(executorURL, new ExecuteStoredJobRequest(job.getID()));
      }
      }
      catch(Exception e)
      {
         // job failed, record that
         log.warn("job " + job + " failed: " + e.getMessage());
         log.debug("job " + job + " failed", e);
         result = new Failure();
      }

      result.setJob(job);
      return result;
   }

   private List runParallelJobs(JobList parallelJobs) throws Exception
   {
      return Runner.runParallelJobs(parallelJobs);

   }

   // Inner classes -------------------------------------------------

}

/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.data;

import org.jboss.logging.Logger;
import org.jboss.jms.perf.framework.protocol.Job;
import org.jboss.jms.perf.framework.Runner;
import org.jboss.jms.perf.framework.protocol.Failure;
import org.jboss.jms.perf.framework.remoting.Coordinator;
import org.jboss.jms.perf.framework.remoting.Result;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Properties;

/**
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

   public void run(Coordinator coordinator) throws Exception
   {
      log.info("");
      log.info("Performance Test \"" + getName() + "\"");

      int executionCounter = 1;

      for(Iterator ei = executions.iterator(); ei.hasNext(); )
      {
         Execution e = (Execution)ei.next();

         log.info("");
         log.info("Execution " + executionCounter++ + " (provider " + e.getProviderName() + ")");

         prepare(e);

         for(Iterator i = jobs.iterator(); i.hasNext(); )
         {
            Object o = i.next();

            if (o instanceof Job)
            {
               Result result = run(coordinator, (Job)o);
               log.info(e.size() + ". " + result);
               e.addMeasurement(result);
            }
            else
            {
               log.info(e.size() + ". PARALLEL");

               List results = runParallel(coordinator, (JobList)o);
               for(Iterator ri = results.iterator(); ri.hasNext(); )
               {
                  log.info("    " + ri.next());
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

   /**
    * Prepares the list of jobs to be ran in a specific execution context.
    */
   private void prepare(Execution e)
   {
      String provider = e.getProviderName();
      Properties providerJNDIProperties = runner.getConfiguration().getJNDIProperties(provider);

      for(Iterator i = jobs.iterator(); i.hasNext(); )
      {
         Object o = i.next();
         if (o instanceof Job)
         {
            ((Job)o).setJNDIProperties(providerJNDIProperties);
         }
         else
         {
            for(Iterator ji = ((JobList)o).iterator(); ji.hasNext(); )
            {
               ((Job)ji.next()).setJNDIProperties(providerJNDIProperties);
            }
         }
      }
   }

   private Result run(Coordinator coordinator, Job job)
   {
      try
      {
         String executorURL = job.getExecutorURL();

         if (executorURL == null)
         {
            throw new Exception("At least on executorURL must be configured on this job");
         }

         Result result = coordinator.sendToExecutor(executorURL, job);
         result.setRequest(job);
         return result;
      }
      catch(Throwable t)
      {
         log.warn("job " + job + " failed: " + t.getMessage());
         log.debug("job " + job + " failed", t);
         return new Failure(job);
      }
   }

   private List runParallel(Coordinator coordinator, JobList parallelJobs)
   {
      List jobExecutors = new ArrayList();

      for(Iterator i = parallelJobs.iterator(); i.hasNext(); )
      {
         JobExecutor je = new JobExecutor(coordinator, (Job)i.next());
         jobExecutors.add(je);
         Thread t = new Thread(je);
         je.setThread(t);
         t.start();
      }

      log.debug("all jobs fired");

      List results = new ArrayList();
      for(Iterator i = jobExecutors.iterator(); i.hasNext(); )
      {
         JobExecutor je = (JobExecutor)i.next();
         try
         {
            je.getThread().join();
         }
         catch (InterruptedException e)
         {}
         results.add(je.getResult());
      }
      return results;
   }

   // Inner classes -------------------------------------------------

   private class JobExecutor implements Runnable
   {
      private Job job;
      private Coordinator coordinator;
      private Result result;
      private Thread thread;

      JobExecutor(Coordinator coordinator, Job job)
      {
         this.coordinator = coordinator;
         this.job = job;
      }

      public void run()
      {
         result = PerformanceTest.this.run(coordinator, job);
      }

      private void setThread(Thread thread)
      {
         this.thread = thread;
      }

      private Thread getThread()
      {
         return thread;
      }

      private Result getResult()
      {
         return result;
      }
   }
}

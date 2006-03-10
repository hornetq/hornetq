/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.perf.framework;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.io.File;

import org.jboss.jms.perf.framework.data.PerformanceTest;
import org.jboss.jms.perf.framework.data.JobList;
import org.jboss.jms.perf.framework.persistence.HSQLDBPersistenceManager;
import org.jboss.jms.perf.framework.persistence.PersistenceManager;
import org.jboss.jms.perf.framework.configuration.Configuration;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version $Revision$
 *
 * $Id$
 */
public class Runner
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Runner.class);

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      Runner runner = new Runner(args);
      runner.start();
      try
      {
         runner.run();
      }
      finally
      {
         runner.stop();
      }
   }

   public static ThroughputResult sendRequestToExecutor(String executorURL, ServerRequest request)
      throws PerfException
   {
      try
      {
         InvokerLocator locator = new InvokerLocator(executorURL);
         Client client = new Client(locator, "executor");
         return (ThroughputResult)client.invoke(request);
      }
      catch (PerfException e)
      {
         throw e;
      }
      catch (Throwable t)
      {
         throw new PerfException("Failed to invoke", t);
      }
   }

   /**
    * Run the jobs concurrently.
    * @return a List of ThroughputResults
    */
   public static List runParallelJobs(JobList parallelJobs) throws PerfException
   {
      List initializers = new ArrayList();

      for(Iterator i = parallelJobs.iterator(); i.hasNext(); )
      {
         Job j = (Job)i.next();
         JobInitializer ji = new JobInitializer(j);
         initializers.add(ji);
         Thread t = new Thread(ji);
         ji.thread = t;
         t.start();
      }

      for(Iterator i = initializers.iterator(); i.hasNext(); )
      {
         JobInitializer ji = (JobInitializer)i.next();
         try
         {
            ji.thread.join();
         }
         catch (InterruptedException e)
         {}

         if (ji.exception != null)
         {
            throw ji.exception;
         }
      }

      // now execute them

      List jobExecutors = new ArrayList();

      for(Iterator i = parallelJobs.iterator(); i.hasNext(); )
      {
         Job j = (Job)i.next();
         JobExecutor je = new JobExecutor(j);
         jobExecutors.add(je);
         Thread t = new Thread(je);
         je.thread = t;
         t.start();
      }

      for(Iterator i = jobExecutors.iterator(); i.hasNext(); )
      {
         JobExecutor je = (JobExecutor)i.next();
         try
         {
            je.thread.join();
         }
         catch (InterruptedException e)
         {}

         if (je.exception != null)
         {
            throw je.exception;
         }
      }

      List results = new ArrayList();

      for(Iterator i = jobExecutors.iterator(); i.hasNext(); )
      {
         JobExecutor ex = (JobExecutor)i.next();
         ThroughputResult result = ex.result;
         result.setJob(ex.job);
         results.add(result);
      }

      return results;
   }

   // Attributes ----------------------------------------------------

   private Configuration configuration;
   protected PersistenceManager pm;
   private String action;

   protected static boolean local; // TODO get rid of this

   // Constructors --------------------------------------------------

   public Runner(String[] args) throws Exception
   {
      action = "measure";
      init(args);
   }

   // Public --------------------------------------------------------

   public Configuration getConfiguration()
   {
      return configuration;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void start() throws Exception
   {
      pm = new HSQLDBPersistenceManager(configuration.getDatabaseURL());
      pm.start();
   }

   protected void stop() throws Exception
   {
      pm.stop();
      pm = null;
   }

   protected void run() throws Exception
   {
      if ("measure".equals(action))
      {
         checkExecutors();
         measure();

      }
      else if ("chart".equals(action))
      {
         chart();
      }
      else
      {
         throw new Exception("Don't know what to do!");
      }
   }

   protected void tearDown() throws PerfException
   {
//      sendRequestToExecutor(slaveURL, new KillRequest());
//      sendRequestToExecutor(slaveURL2, new KillRequest());
   }
   
   // Private -------------------------------------------------------

   private void init(String[] args) throws Exception
   {
      String configFileName = null;

      for(int i = 0; i < args.length; i++)
      {
         if ("-config".equals(args[i]))
         {
            if (i == args.length - 1)
            {
               throw new Exception("A configuration file name must follow after -config!");
            }
            configFileName = args[i + 1];
         }
         else if ("-action".equals(args[i]))
         {
            if (i == args.length - 1)
            {
               throw new Exception("An action name must follow after -action!");
            }
            action = args[i + 1];
         }
      }

      if (action == null)
      {
         throw new Exception("No action specified!. Use -action <measure|chart|...>");
      }

      if (configFileName == null)
      {
         throw new Exception("A configuration file name must be specified. Example: -config perf.xml");
      }

      File conf = new File(configFileName);

      if (!conf.isFile() || !conf.canRead())
      {
         throw new Exception("The file " + configFileName + " does not exist or cannot be read!");
      }

      configuration = new Configuration(this, conf);

   }

   private void measure() throws Exception
   {
      for(Iterator i = configuration.getPerformanceTests().iterator(); i.hasNext(); )
      {
         PerformanceTest pt = (PerformanceTest)i.next();
         pt.run();
         pm.savePerformanceTest(pt);
      }
   }

   private void chart() throws Exception
   {
      Charter charter = new Charter(pm, configuration.getReportDirectory());
      charter.run();
      log.info("charts created");
   }

   private void checkExecutors() throws Exception
   {
      for(Iterator i = configuration.getExecutorURLs().iterator(); i.hasNext(); )
      {
         String executorURL = (String)i.next();

         try
         {
            Runner.sendRequestToExecutor(executorURL, new ResetRequest());
            log.info("executor " + executorURL + " on-line and reset");
         }
         catch(Exception e)
         {
            log.error("executor " + executorURL + " failed", e);
            throw new Exception("executor check failed");
         }
      }

      if (configuration.isStartExecutors())
      {
         throw new Exception("Not able to dynamically start executors yet!");
      }
   }

   // Inner classes -------------------------------------------------

   static class JobInitializer implements Runnable
   {
      Job job;
      Thread thread;
      PerfException exception;

      JobInitializer(Job job)
      {
         this.job = job;
      }
      
      public void run()
      {
         try
         {
            if (local)
            {
               job.initialize();
            }
            else
            {
               sendRequestToExecutor(job.getExecutorURL(), new StoreJobRequest(job));
            }
         }
         catch (PerfException e)
         {
            log.error("Failed to intialize job", e);
            exception = e;
         }
      }
   }

   static class JobExecutor implements Runnable
   {
      Job job;
      ThroughputResult result;
      Thread thread;
      PerfException exception;

      JobExecutor(Job job)
      {
         this.job = job;
      }

      public void run()
      {
         try
         {
            if (local)
            {
               result = job.execute();
            }
            else
            {
               result =
                  sendRequestToExecutor(job.getExecutorURL(), new ExecuteStoredJobRequest(job.getID()));
            }
         }
         catch (Exception e)
         {
            // job failed, record that
            log.warn("job " + job + " failed: " + e.getMessage());
            log.debug("job " + job + " failed", e);
            result = new Failure();
         }
      }
   }
}

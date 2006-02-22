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

import java.util.Date;
import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.Session;

import org.jboss.jms.perf.framework.data.Measurement;
import org.jboss.jms.perf.framework.data.PerformanceTest;
import org.jboss.jms.perf.framework.data.Execution;
import org.jboss.jms.perf.framework.data.Datapoint;
import org.jboss.jms.perf.framework.factories.MessageMessageFactory;
import org.jboss.jms.perf.framework.persistence.HSQLDBPersistenceManager;
import org.jboss.jms.perf.framework.persistence.PersistenceManager;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;

/**
 * 
 * A Runner.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class Runner
{
   private static final Logger log = Logger.getLogger(Runner.class);   
    
   protected boolean local;
   
   protected Properties jndiProperties;
   
   //TODO - configure these in the ant script
   protected String slaveURL1 = "socket://localhost:1234";
   
   protected String slaveURL2 = "socket://localhost:1235";
   
   protected String dbURL = "jdbc:hsqldb:./perfResultsDB";
   
   protected String providerName = "JBossMQ";
   
   protected PersistenceManager pm;
   
   public Runner()
   {
      //Currently hardcoded
      jndiProperties = new Properties();
      
      jndiProperties.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
      
      jndiProperties.put("java.naming.provider.url", "jnp://localhost:1099");
      
      jndiProperties.put("java.naming.factory.url.pkg", "org.jboss.naming:org.jnp.interfaces");      
   }
   
   public static void main(String[] args)
   {
      new Runner().run();
   }
   
   public void run()
   {
      try
      {         
         pm = new HSQLDBPersistenceManager(dbURL);
         pm.start();
         
         test1();
                 
         tearDown();
         
         pm.stop();
      }
      catch (PerfException e)
      {
         log.error("Failed to run performance tests", e);
      }
   }
   
   protected void test1() throws PerfException
   {
      //Non transacted, non-persistent, send/receive on queue, 0K message
      
      log.info("Running test1");
      
      //drain
      
      Execution execution = createExecution("Queue1");     
            
      DrainJob drainJob = new DrainJob(jndiProperties, "/queue/testQueue", "/ConnectionFactory",
            null, null);
        
      double targetRate = 500;
      
      while (targetRate < 4000)
      {
         log.info("Trying with target rate=" + targetRate);
                  
         runJob(slaveURL1, drainJob);
                  
         SenderJob senderJob = new SenderJob(jndiProperties, "/queue/testQueue", "/ConnectionFactory",
               1, 1, false, 0, 30000, false, 0, new MessageMessageFactory(), DeliveryMode.NON_PERSISTENT,
               targetRate);
         
         //Make sure the receive job runs shorter than the send job, else timings are out
         //since the receive job waits and timesout on receive
         
         ReceiverJob receiverJob = new ReceiverJob(jndiProperties, "/queue/testQueue", "/ConnectionFactory",
               1, 1, false, 0, 27000, Session.AUTO_ACKNOWLEDGE, null, null, false, false, null);
         
         ThroughputResult[] results = runJobs(new String[] { slaveURL1, slaveURL2}, new Job[] { senderJob, receiverJob } );
         
         log.info("Target send rate=" + targetRate + ", actual Send=" + results[0].getThroughput() +
               ", acual receive=" + results[1].getThroughput());
         
         double sendRate = results[0].getThroughput();
         
         double receiveRate = results[1].getThroughput();
         
         Datapoint datapoint = new Datapoint(null);
         
         Measurement measureSend = new Measurement("sendRate", new Double(sendRate));
         
         Measurement measureReceive = new Measurement("receiveRate", new Double(receiveRate));
         
         datapoint.addMeasurement(measureSend);
         
         datapoint.addMeasurement(measureReceive);
         
         execution.addDatapoint(datapoint);
         
         targetRate += 150;
       
      }                  
      
      pm.saveExecution(execution);
      
      log.info("Saved execution");
      
   }
   
   protected void tearDown() throws PerfException
   {
      sendRequestToSlave(slaveURL1, new KillRequest());
      
      sendRequestToSlave(slaveURL2, new KillRequest());
   }
   

   protected ThroughputResult runJob(String slaveURL, Job job) throws PerfException
   {
      if (local)
      {
         job.initialize();
         return job.execute();
      }
      else
      {
         sendRequestToSlave(slaveURL, new SubmitJobRequest(job));
         
         return sendRequestToSlave(slaveURL, new ExecuteJobRequest(job.getId()));
      }
   }
   
   /*
    * Run the jobs concurrently
    */
   protected ThroughputResult[] runJobs(String[] slaveURLS, Job[] jobs) throws PerfException
   {      
      //First initialize them
      
      JobInitializer[] initializers = new JobInitializer[jobs.length];
      for (int i = 0; i < jobs.length; i++)
      {
         initializers[i] = new JobInitializer(slaveURLS[i], jobs[i]);
         Thread t = new Thread(initializers[i]);
         initializers[i].thread = t;
         t.start();
      }
      
      for (int i = 0; i < jobs.length; i++)
      {
         try
         {
            initializers[i].thread.join();            
         }
         catch (InterruptedException e)
         {}
         
         if (initializers[i].exception != null)
         {
            throw initializers[i].exception;
         }                     
      } 
            
      //Now execute them
      
      JobExecutor[] runners = new JobExecutor[jobs.length];
      for (int i = 0; i < jobs.length; i++)
      {
         runners[i] = new JobExecutor(slaveURLS[i], jobs[i]);
         Thread t = new Thread(runners[i]);
         runners[i].thread = t;
         t.start();
      }
      
      for (int i = 0; i < jobs.length; i++)
      {
         try
         {
            runners[i].thread.join();
         }
         catch (InterruptedException e)
         {}
         if (runners[i].exception != null)
         {
            throw runners[i].exception;
         }
      } 
      ThroughputResult[] results = new ThroughputResult[jobs.length];
      for (int i = 0; i < jobs.length; i++)
      {
         results[i] = runners[i].result;
      }
      return results;      
   }
   
   protected ThroughputResult sendRequestToSlave(String slaveURL, ServerRequest request) throws PerfException
   {
      try
      {
         InvokerLocator locator = new InvokerLocator(slaveURL);
         
         Client client = new Client(locator, "perftest");
               
         Object res = client.invoke(request);
                           
         return (ThroughputResult)res; 
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
   
   protected Execution createExecution(String benchmarkName)
   {
      Execution exec = new Execution(new PerformanceTest(benchmarkName), new Date(), providerName);
      
      return exec;
   }
   
   class JobExecutor implements Runnable
   {
      Job job;
      
      String slaveURL;
      
      ThroughputResult result;
      
      Thread thread;
      
      PerfException exception;

      JobExecutor(String slaveURL, Job job)
      {
         this.slaveURL = slaveURL;
         
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
               result = sendRequestToSlave(slaveURL, new ExecuteJobRequest(job.getId()));
            }
         }
         catch (PerfException e)
         {
            log.error("Failed to execute job", e);
            exception = e;
         }
      }
   }
   
   class JobInitializer implements Runnable
   {
      Job job;
      
      String slaveURL;
       
      Thread thread;
      
      PerfException exception;

      JobInitializer(String slaveURL, Job job)
      {
         this.slaveURL = slaveURL;
         
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
               sendRequestToSlave(slaveURL, new SubmitJobRequest(job));
            }
         }
         catch (PerfException e)
         {
            log.error("Failed to intialize job", e);
            exception = e;
         }
      }
   }
}

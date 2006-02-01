
/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.jms.Connection;

import org.jboss.logging.Logger;

/**
 * 
 * A BaseThroughputJob.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public abstract class BaseThroughputJob extends BaseJob
{
   private transient static final Logger log = Logger.getLogger(BaseThroughputJob.class);
   
   /* Number of Connections to use */
   protected int numConnections;
   
   /* Number of concurrent session to use - sessions use connections in round-robin fashion */
   protected int numSessions;
   
   /* Is the session transacted? */
   protected boolean transacted;
    
   /* Array of connections to use */
   protected Connection[] conns;
   
   protected int connIndex;
   
   protected int transactionSize;
   
   protected abstract Servitor createServitor(int numMessages);
    
   protected int numMessages;
   
   Thread[] servitorThreads;
   
   Servitor[] servitors;
   
   public BaseThroughputJob(String slaveURL, Properties jndiProperties, String destinationName,
         String connectionFactoryJndiName, int numConnections,
         int numSessions, boolean transacted, int transactionSize, int numMessages)
   {
      super(slaveURL, jndiProperties, destinationName, connectionFactoryJndiName);
      
      this.numConnections = numConnections;
      
      this.numSessions = numSessions;
      
      this.transacted = transacted;
      
      this.transactionSize = transactionSize;
      
      this.numMessages = numMessages;
      
      servitorThreads = new Thread[numSessions];      
      
      servitors = new Servitor[numSessions];      
   }
    
   public void initialize() throws PerfException
   {
      try
      {
         
         super.initialize();
         
         conns = new Connection[numConnections];
         
         for (int i = 0; i < numConnections; i++)
         {
            conns[i] = cf.createConnection();
            
            conns[i].start();
         }
               
         for (int i = 0; i < numSessions; i++)
         {
            Servitor servitor = createServitor(numMessages);
            
            servitor.init();
            
            servitors[i] = servitor;
            
            servitorThreads[i] = new Thread(servitors[i]);
         }      
                           
         log.info("Initialized job: " + this);
      }
      catch (Exception e)
      {
         log.error("Failed to initialize", e);
         throw new PerfException("Failed to initialize", e);
      }
   }
      
   public JobResult execute() throws PerfException
   {
      try
      {
         
         log.info("==============Executing job:" + this);
         
         logInfo();
         
         JobResult res = runTest();         
         
         tearDown();
         
         log.info("================Executed and toreDown");     
         
         return res;
      }
      catch (Exception e)
      {
         log.error("Failed to execute", e);
         throw new PerfException("Failed to execute", e);
      }
   }
   
     
   protected JobResult runTest() throws PerfException
   {
      boolean failed = false;
            
      long startTime = System.currentTimeMillis();
            
      for (int i = 0; i < numSessions; i++)
      {         
         servitorThreads[i].start();
      }      
      
      for (int i = 0; i < numSessions; i++)
      {
         try
         {
            servitorThreads[i].join();
         }
         catch (InterruptedException e)
         {
            throw new PerfException("Thread interrupted");
         }
      }
      
      long endTime = System.currentTimeMillis();

      List throwablesList = new ArrayList();
      
      for (int i = 0; i < numSessions; i++)
      {
         Servitor servitor = servitors[i];
         
         
         servitor.deInit();
         if (servitor.isFailed())           
         {
            failed = true;
            if (servitor.getThrowable() != null)
            {
               throwablesList.add(servitor.getThrowable());
            }
         }
      } 
      
      Throwable[] throwables = null;
      
      if (!throwablesList.isEmpty())
      {
         throwables = new Throwable[throwablesList.size()];
         
         Iterator iter = throwablesList.iterator();
         int i = 0;
         while (iter.hasNext())
         {
            throwables[i++] = (Throwable)iter.next();
         }         
      }
      
      if (failed)
      {
         throw new PerfException("Test failed");
      }
      
      return new JobResult(startTime, endTime);
   }
   
   abstract class AbstractServitor implements Servitor
   {
      protected boolean failed; 
      
      protected int numMessages;
      
      protected Throwable throwable;
      
      AbstractServitor(int numMessages)
      {         
         this.numMessages = numMessages;
      }              
      
      public boolean isFailed()
      {
         return failed;
      }
      
      public Throwable getThrowable()
      {
         return throwable;
      }
   }
   
   protected synchronized Connection getNextConnection()
   {
      return conns[connIndex++ % conns.length];
   }
     

   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      for (int i = 0; i < numConnections; i++)
      {         
         conns[i].close();
      }
   }
   

   protected void logInfo()
   {
      super.logInfo();      
      log.trace("Number of connections: " + numConnections);
      log.trace("Numbe of concurrent sessions: " + numSessions);
      log.trace("Transacted?: " + transacted);
      log.trace("Transaction size: " + transactionSize);
      log.trace("Num messages:" + numMessages);
   }
   
 
   /**
    * Set the numConnections.
    * 
    * @param numConnections The numConnections to set.
    */
   public void setNumConnections(int numConnections)
   {
      this.numConnections = numConnections;
   }


   /**
    * Set the numSessions.
    * 
    * @param numSessions The numSessions to set.
    */
   public void setNumSessions(int numSessions)
   {
      this.numSessions = numSessions;
   }


   /**
    * Set the numMessages.
    * 
    * @param numMessages The numMessages to set.
    */
   public void setNumMessages(int numMessages)
   {
      this.numMessages = numMessages;
   }

   /**
    * Set the transacted.
    * 
    * @param transacted The transacted to set.
    */
   public void setTransacted(boolean transacted)
   {
      this.transacted = transacted;
   }


   /**
    * Set the transactionSize.
    * 
    * @param transactionSize The transactionSize to set.
    */
   public void setTransactionSize(int transactionSize)
   {
      this.transactionSize = transactionSize;
   }


   

}
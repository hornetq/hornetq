
/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework;

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
   
   protected abstract Servitor createServitor(long testTime);
    
   protected long testTime;
   
   Thread[] servitorThreads;
   
   Servitor[] servitors;
   
   public BaseThroughputJob(Properties jndiProperties, String destinationName,
         String connectionFactoryJndiName, int numConnections,
         int numSessions, boolean transacted, int transactionSize, long testTime)
   {
      super(jndiProperties, destinationName, connectionFactoryJndiName);
      
      this.numConnections = numConnections;
      
      this.numSessions = numSessions;
      
      this.transacted = transacted;
      
      this.transactionSize = transactionSize;
      
      this.testTime = testTime;      
      
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
            Servitor servitor = createServitor(testTime);
            
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
      
   public ThroughputResult execute() throws PerfException
   {
      try
      {         
         log.info("==============Executing job:" + this);
         
         ThroughputResult res = runTest();         
         
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
        
   protected ThroughputResult runTest() throws PerfException
   {
      boolean failed = false;
                    
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
      
      long totalTime = 0;
      
      long totalMessages = 0;
      
      for (int i = 0; i < numSessions; i++)
      {
         Servitor servitor = servitors[i];
                  
         servitor.deInit();
         
         if (servitor.isFailed())           
         {
            failed = true;       
            
            break;
         }
         else
         {
            totalTime += servitor.getTime();
            
            totalMessages += servitor.getMessages();
         }
      }       
      
      if (failed)
      {
         throw new PerfException("Test failed");
      }
      
      return new ThroughputResult(totalTime, totalMessages);
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
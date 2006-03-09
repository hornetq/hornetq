
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
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public abstract class BaseThroughputJob extends BaseJob
{
   private transient static final Logger log = Logger.getLogger(BaseThroughputJob.class);
   
   protected int numConnections;
   
   /* Number of concurrent session to use - sessions use connections in round-robin fashion */
   protected int numSessions;
   
   protected boolean transacted;
    
   protected Connection[] conns;
   
   protected int connIndex;
   
   protected int transactionSize;
   
   protected abstract Servitor createServitor(long duration);
    
   Thread[] servitorThreads;
   
   Servitor[] servitors;

   public BaseThroughputJob()
   {
      this(null, null, null, 1, 1, false, 0, Long.MAX_VALUE, 0);
   }

   public BaseThroughputJob(Properties jndiProperties,
                            String destinationName,
                            String connectionFactoryJndiName,
                            int numConnections,
                            int numSessions,
                            boolean transacted,
                            int transactionSize,
                            long duration,
                            int rate)
   {
      super(jndiProperties, destinationName, connectionFactoryJndiName);
      
      this.numConnections = numConnections;
      this.numSessions = numSessions;
      this.transacted = transacted;
      this.transactionSize = transactionSize;
      this.duration = duration;
      servitorThreads = new Thread[numSessions];
      servitors = new Servitor[numSessions];
      this.rate = rate;
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
            Servitor servitor = createServitor(duration);
            
            servitor.init();
            servitors[i] = servitor;
            servitorThreads[i] = new Thread(servitors[i]);
         }      
                           
         log.info("initialized " + this);
      }
      catch (Exception e)
      {
         log.error("Failed to initialize", e);
         throw new PerfException("Failed to initialize", e);
      }
   }
      
   public ThroughputResult execute() throws PerfException
   {

      if (log.isTraceEnabled()) { log.trace(this + " executing ..."); }

      try
      {         
         ThroughputResult res = runTest();
         tearDown();
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

      if (log.isTraceEnabled()) { log.trace(this + " runs test with " + numSessions + " parallel sessions"); }
                    
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
   
   public void setNumConnections(int numConnections)
   {
      this.numConnections = numConnections;
   }

   public void setNumSessions(int numSessions)
   {
      this.numSessions = numSessions;
   }

   public void setTransacted(boolean transacted)
   {
      this.transacted = transacted;
   }

   public void setTransactionSize(int transactionSize)
   {
      this.transactionSize = transactionSize;
   }

}
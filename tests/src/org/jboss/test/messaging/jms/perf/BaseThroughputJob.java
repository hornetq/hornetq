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
package org.jboss.test.messaging.jms.perf;

import javax.jms.Connection;

import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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
    
   protected boolean failed;
   
   protected int numMessages;
   
   protected long testTime;
   
   protected long startTime;
   
   protected long initTime;
   
   
   
   public Object getResult()
   {
      return !failed ? new JobTimings(initTime, testTime) : null;
   }
   
   public void run()
   {
      try
      {
         startTime = System.currentTimeMillis();
         
         log.info("==============Running job:" + this);
         setup();
         logInfo();
         runTest();         
         tearDown();
         log.info("================Finished running job");
      }
      catch (Exception e)
      {
         log.error("Failed to run test", e);
      }
   }
   
     
   protected void runTest() throws Exception
   {
      failed = false;
      
      Thread[] servitorThreads = new Thread[numSessions];      
      Servitor[] servitors = new Servitor[numSessions];
      
      for (int i = 0; i < numSessions; i++)
      {
         Servitor servitor = createServitor(numMessages);
         servitor.init();
         servitors[i] = servitor;
      }      
            
      long testStartTime = System.currentTimeMillis();
      initTime = testStartTime - startTime;
      
      for (int i = 0; i < numSessions; i++)
      {         
         servitorThreads[i] = new Thread(servitors[i]);
         servitorThreads[i].start();
      }      
      
      for (int i = 0; i < numSessions; i++)
      {
         servitorThreads[i].join();
      }
      
      long endTime = System.currentTimeMillis();
      
      testTime = endTime - testStartTime;
      
      for (int i = 0; i < numSessions; i++)
      {
         Servitor servitor = servitors[i];
         servitor.deInit();
         if (servitor.isFailed())
         {
            failed = true;
         }
      }    
      
      if (testTime == 0)
      {
         failed = true;
         log.error("!!!!!!!!!!!!!! testTime is zero");
      }
   }
   
   abstract class AbstractServitor implements Servitor
   {
      protected boolean failed; 
      
      protected int numMessages;
      
      AbstractServitor(int numMessages)
      {         
         this.numMessages = numMessages;
      }              
      
      public boolean isFailed()
      {
         return failed;
      }
   }
   
   protected synchronized Connection getNextConnection()
   {
      return conns[connIndex++ % conns.length];
   }
     
   protected void setup() throws Exception
   {
      super.setup();
      
      conns = new Connection[numConnections];
      
      for (int i = 0; i < numConnections; i++)
      {
         conns[i] = cf.createConnection();
         conns[i].start();
      }
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
   
   
   public BaseThroughputJob(String slaveURL, String serverURL, String destinationName, int numConnections,
         int numSessions, boolean transacted, int transactionSize, int numMessages)
   {
      super(slaveURL, serverURL, destinationName);
      this.numConnections = numConnections;
      this.numSessions = numSessions;
      this.transacted = transacted;
      this.transactionSize = transactionSize;
      this.numMessages = numMessages;
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
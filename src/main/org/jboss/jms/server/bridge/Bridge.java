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
package org.jboss.jms.server.bridge;

import java.util.Iterator;
import java.util.LinkedList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;

/**
 * 
 * A Bridge
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class Bridge implements MessagingComponent
{
   private static final Logger log;
   
   private static boolean trace;
   
   //Quality of service modes
   
   public static final int QOS_AT_MOST_ONCE = 0;
   
   public static final int QOS_DUPLICATES_OK = 1;
   
   public static final int QOS_ONCE_AND_ONLY_ONCE = 2;
   
   /*
    * 
    * Quality of service (QoS) levels
    * ===============================
    * 
    * QOS_AT_MOST_ONCE
    * ----------------
    * 
    * With this QoS mode messages will reach the destination from the source at most once.
    * The messages are consumed from the source and acknowledged
    * before sending to the destination. Therefore there is a possibility that if failure occurs between
    * removing them from the source and them arriving at the destination they could be lost. Hence delivery
    * will occur at most once.
    * This mode is avilable for both persistent and non persistent messages.
    * 
    * QOS_DUPLICATES_OK
    * -----------------
    * 
    * With this QoS mode, the messages are consumed from the source and then acknowledged
    * after they have been successfully sent to the destination. Therefore there is a possibility that if
    * failure occurs after sending to the destination but before acknowledging them, they could be sent again
    * when the system recovers. I.e. the destination might receive duplicates after a failure.
    * This mode is available for both persistent and non persistent messages.
    * 
    * QOS_ONCE_AND_ONLY_ONCE
    * ----------------------
    * 
    * This QoS mode ensures messages will reach the destination from the source once and only once.
    * (Sometimes this mode is known as "exactly once").
    * If both the source and the destination are on the same JBoss Messaging server instance then this can 
    * be achieved by sending and acknowledging the messages in the same local transaction.
    * If the source and destination are on different servers this is achieved by enlisting the sending and consuming
    * sessions in a JTA transaction. The JTA transaction is controlled by JBoss Transactions JTA implementation which
    * is a fully recovering transaction manager, thus providing a very high degree of durability.
    * If JTA is required then both supplied connection factories need to be XAConnectionFactory implementations.
    * This mode is only available for persistent messages.
    * This is likely to be the slowest mode since it requires extra persistence for the transaction logging.
    * 
    * Note:
    * For a specific application it may possible to provide once and only once semantics without using the
    * QOS_ONCE_AND_ONLY_ONCE QoS level. This can be done by using the QOS_DUPLICATES_OK mode and then checking for
    * duplicates at the destination and discarding them. Some JMS servers provide automatic duplicate message detection
    * functionality, or this may be possible to implement on the application level by maintaining a cache of received
    * message ids on disk and comparing received messages to them. The cache would only be valid
    * for a certain period of time so this approach is not as watertight as using QOS_ONCE_AND_ONLY_ONCE but
    * may be a good choice depending on your specific application.
    * 
    * 
    */
   
   static
   {
      log = Logger.getLogger(Bridge.class);
      
      trace = log.isTraceEnabled();
   }

   private String sourceUsername;
   
   private String sourcePassword;
   
   private String destUsername;
   
   private String destPassword;
   
   private TransactionManager tm;
   
   private String selector;
   
   private long failureRetryInterval;
   
   private int maxRetries;
   
   private int qualityOfServiceMode;
   
   private int maxBatchSize;
   
   private long maxBatchTime;
   
   private String subName;
   
   private String clientID;
   
   private boolean started;
   
   private LinkedList messages;
   
   private Object lock;
   
   private ConnectionFactoryFactory sourceCfFactory;
   
   private ConnectionFactoryFactory destCfFactory;
   
   private Connection connSource; 
   
   private Connection connDest;
   
   private Destination destSource;
   
   private Destination destDest;
   
   private Session sessSource;
   
   private Session sessDest;
   
   private MessageConsumer consumer;
   
   private MessageProducer producer;
        
   private BatchTimeChecker timeChecker;
   
   private Thread checkerThread;
   
   private long batchExpiryTime;
   
   private boolean paused;         
   
   private Transaction tx;  
   
   private boolean manualAck;
   
   private boolean manualCommit;
      
   /*
    * This constructor is used when source and destination are on different servers
    */
   public Bridge(ConnectionFactoryFactory sourceCfFactory, ConnectionFactoryFactory destCfFactory,
                 Destination destSource, Destination destDest,         
                 String sourceUsername, String sourcePassword,
                 String destUsername, String destPassword,
                 String selector, long failureRetryInterval,
                 int maxRetries,
                 int qosMode,
                 int maxBatchSize, long maxBatchTime,
                 String subName, String clientID)
   {
      if (sourceCfFactory == null)
      {
         throw new IllegalArgumentException("sourceCfFactory cannot be null");
      }
      if (destCfFactory == null)
      {
         throw new IllegalArgumentException("destCfFactory cannot be null");
      }
      if (destSource == null)
      {
         throw new IllegalArgumentException("destSource cannot be null");
      }
      if (destDest == null)
      {
         throw new IllegalArgumentException("destDest cannot be null");
      }
      if (failureRetryInterval < 0 && failureRetryInterval != -1)
      {
         throw new IllegalArgumentException("failureRetryInterval must be > 0 or -1 to represent no retry");
      }
      if (maxRetries < 0)
      {
         throw new IllegalArgumentException("maxRetries must be >= 0");
      }
      if (failureRetryInterval == -1 && maxRetries > 0)
      {
         throw new IllegalArgumentException("If failureRetryInterval == -1 maxRetries must be 0");
      }
      if (maxBatchSize < 1)
      {
         throw new IllegalArgumentException("maxBatchSize must be >= 1");
      }
      if (maxBatchTime < 1 && maxBatchTime != -1)
      {
         throw new IllegalArgumentException("maxBatchTime must be >= 1 or -1 to represent unlimited batch time");
      }
      if (qosMode != QOS_AT_MOST_ONCE && qosMode != QOS_DUPLICATES_OK && qosMode != QOS_ONCE_AND_ONLY_ONCE)
      {
         throw new IllegalArgumentException("Invalid QoS mode " + qosMode);
      }
      
      this.sourceCfFactory = sourceCfFactory;
      
      this.destCfFactory = destCfFactory;
      
      this.destSource = destSource;
      
      this.destDest = destDest;
      
      this.sourceUsername = sourceUsername;
      
      this.sourcePassword = sourcePassword;
      
      this.destUsername = destUsername;
      
      this.destPassword = destPassword;
      
      this.selector = selector;
      
      this.failureRetryInterval = failureRetryInterval;
      
      this.maxRetries = maxRetries;
      
      this.qualityOfServiceMode = qosMode;
      
      this.maxBatchSize = maxBatchSize;
      
      this.maxBatchTime = maxBatchTime;
      
      this.subName = subName;
      
      this.clientID = clientID;
      
      this.messages = new LinkedList();      
      
      this.lock = new Object();
      
      if (trace)
      {
         log.trace("Created " + this);
      }
   }
   
      
   // MessagingComponent overrides --------------------------------------------------
        
   public synchronized void start() throws Exception
   {
      if (started)
      {
         log.warn("Attempt to start, but is already started");
         return;
      }
      
      if (trace) { log.trace("Starting " + this); }         
      
      boolean ok = setupJMSObjectsWithRetry();
      
      if (ok)
      {         
         started = true;
         
         if (maxBatchTime != -1)
         {
            if (trace) { log.trace("Starting time checker thread"); }
                     
            timeChecker = new BatchTimeChecker();
            
            checkerThread = new Thread(timeChecker);
            
            batchExpiryTime = System.currentTimeMillis() + maxBatchTime;
            
            checkerThread.start();
            
            if (trace) { log.trace("Started time checker thread"); }
         }            
         
         if (trace) { log.trace("Started " + this); }
      }
      else
      {
         log.warn("Failed to start bridge");
      }
   }
   
   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         log.warn("Attempt to stop, but is already stopped");
         return;
      }
      
      if (trace) { log.trace("Stopping " + this); }
      
      synchronized (lock)
      {
         started = false;          
         
         //This must be inside sync block
         if (checkerThread != null)
         {
            checkerThread.interrupt();
         }
      }
            
      //This must be outside sync block
      if (checkerThread != null)
      {  
         if (trace) { log.trace("Waiting for checker thread to finish");}
         
         checkerThread.join();
         
         if (trace) { log.trace("Checker thread has finished"); }
      }
      
      connSource.close();
      
      connDest.close();

      if (tx != null)
      {
         //Terminate any transaction
         if (trace) { log.trace("Rolling back remaining tx"); }
         
         tx.rollback();
         
         if (trace) { log.trace("Rolled back remaining tx"); }
      }
      
      if (trace) { log.trace("Stopped " + this); }
   }
   
   // Public ---------------------------------------------------------------------------
   
   public synchronized void pause() throws Exception
   {
      if (trace) { log.trace("Pausing " + this); }
      
      synchronized (lock)
      {
         paused = true;
         
         connSource.stop();
      }
      
      if (trace) { log.trace("Paused " + this); }
   }
   
   public synchronized void resume() throws Exception
   {
      if (trace) { log.trace("Resuming " + this); }
      
      synchronized (lock)
      {
         paused = false;
         
         connSource.start();
      }
      
      if (trace) { log.trace("Resumed " + this); }
   }
   
   // Private -------------------------------------------------------------------
   
   private void enlistResources(Transaction tx) throws Exception
   {
      if (trace) { log.trace("Enlisting resources in tx"); }
      
      XAResource resSource = ((XASession)sessSource).getXAResource();
      
      tx.enlistResource(resSource);
      
      XAResource resDest = ((XASession)sessDest).getXAResource();
      
      tx.enlistResource(resDest);
      
      if (trace) { log.trace("Enlisted resources in tx"); }
   }
   
   private void delistResources(Transaction tx) throws Exception
   {
      if (trace) { log.trace("Delisting resources from tx"); }
      
      XAResource resSource = ((XASession)sessSource).getXAResource();
      
      tx.delistResource(resSource, XAResource.TMSUCCESS);
      
      XAResource resDest = ((XASession)sessDest).getXAResource();
      
      tx.delistResource(resDest, XAResource.TMSUCCESS);
      
      if (trace) { log.trace("Delisted resources from tx"); }
   }
   
   private Transaction startTx() throws Exception
   {
      if (trace) { log.trace("Starting JTA transaction"); }
      
      TransactionManager tm = getTm();
      
      tm.begin();
      
      Transaction tx = tm.getTransaction();
      
      //Remove the association between current thread - we don't want it
      //we will be committing /rolling back directly on the transaction object
      
      tm.suspend();
      
      if (trace) { log.trace("Started JTA transaction"); }
      
      return tx;
   }
   
   private TransactionManager getTm()
   {
      if (tm == null)
      {
//         tm = TransactionManagerLocator.getInstance().locate();
//         
//         if (tm == null)
//         {
//            throw new IllegalStateException("Cannot locate a transaction manager");
//         }
         
         tm = com.arjuna.ats.jta.TransactionManager.transactionManager();
      }
      
      return tm;
   }
   
   private Connection createConnection(String username, String password, ConnectionFactoryFactory cff)
      throws Exception
   {
      Connection conn;
      
      ConnectionFactory cf = cff.createConnectionFactory();
      
      if (qualityOfServiceMode == QOS_ONCE_AND_ONLY_ONCE &&
          !(cf instanceof XAConnectionFactory))
      {
         throw new IllegalArgumentException("Connection factory must be XAConnectionFactory");
      }
      
      if (username == null)
      {
         if (qualityOfServiceMode == QOS_ONCE_AND_ONLY_ONCE)
         {
            if (trace) { log.trace("Creating an XA connection"); }
            conn = ((XAConnectionFactory)cf).createXAConnection();
         }
         else
         {
            if (trace) { log.trace("Creating a non XA connection"); }
            conn = cf.createConnection();            
         }
      }
      else
      {
         if (qualityOfServiceMode == QOS_ONCE_AND_ONLY_ONCE)
         {
            if (trace) { log.trace("Creating an XA connection"); }
            conn = ((XAConnectionFactory)cf).createXAConnection(username, password);
         }
         else
         {
            if (trace) { log.trace("Creating a non XA connection"); }
            conn = cf.createConnection(username, password);            
         }  
      }
      return conn;
   }
   
   private boolean setupJMSObjects()
   {
      try
      {
         connSource = createConnection(sourceUsername, sourcePassword, sourceCfFactory);
         
         connDest = createConnection(destUsername, destPassword, destCfFactory);
         
         if (clientID != null)
         {
            connSource.setClientID(clientID);
         }
          
         Session sess;
         
         if (qualityOfServiceMode == QOS_ONCE_AND_ONLY_ONCE)
         {
            //Create an XASession for consuming from the source
            if (trace) { log.trace("Creating XA source session"); }
            sessSource = ((XAConnection)connSource).createXASession();
            
            sess = ((XASession)sessSource).getSession();
         }
         else
         {
            if (trace) { log.trace("Creating non XA source session"); }
            
            //Create a standard session for consuming from the source
            
            //If the QoS is at_most_once, and max batch size is 1 then we use AUTO_ACKNOWLEDGE
            //If the QoS is at_most_once, and max batch size > 1 or -1, then we use CLIENT_ACKNOWLEDGE
            //We could use CLIENT_ACKNOWLEDGE for both the above but AUTO_ACKNOWLEGE may be slightly more
            //performant in some implementations that manually acking every time but it really depends
            //on the implementation.
            //We could also use local transacted for both the above but don't for the same reasons.
            
            //If the QoS is duplicates_ok, we use CLIENT_ACKNOWLEDGE
            //We could use local transacted, whether one is faster than the other probably depends on the
            //messaging implementation but there's probably not much in it
            
            int ackMode;
            if (qualityOfServiceMode == QOS_AT_MOST_ONCE && maxBatchSize == 1)
            {
               ackMode = Session.AUTO_ACKNOWLEDGE;
            }
            else
            {
               ackMode = Session.CLIENT_ACKNOWLEDGE;
               
               manualAck = true;
            }
            
            sessSource = connSource.createSession(false, ackMode);
            
            sess = sessSource;
         }
            
         if (subName == null)
         {
            if (selector == null)
            {
               consumer = sess.createConsumer(destSource);
            }
            else
            {
               consumer = sess.createConsumer(destSource, selector, false);
            }
         }
         else
         {
            //Durable subscription
            if (selector == null)
            {
               consumer = sess.createDurableSubscriber((Topic)destSource, subName);
            }
            else
            {
               consumer = sess.createDurableSubscriber((Topic)destSource, subName, selector, false);
            }
         }
         
         if (qualityOfServiceMode == QOS_ONCE_AND_ONLY_ONCE)
         {
            if (trace) { log.trace("Creating XA dest session"); }
            
            //Create an XA sesion for sending to the destination
            
            sessDest = ((XAConnection)connDest).createXASession();
            
            sess = ((XASession)sessDest).getSession();
         }
         else
         {
            if (trace) { log.trace("Creating non XA dest session"); }
            
            //Create a standard session for sending to the destination
            
            //If maxBatchSize == 1 we just create a non transacted session, otherwise we
            //create a transacted session for the send, since sending the batch in a transaction
            //is likely to be more efficient than sending messages individually
            
            manualCommit = maxBatchSize == 1;
            
            sessDest = connDest.createSession(manualCommit, manualCommit ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
            
            sess = sessDest;
         }
         
         if (qualityOfServiceMode == QOS_ONCE_AND_ONLY_ONCE)
         {
            if (trace) { log.trace("Starting JTA transaction"); }
            
            tx = startTx();
            
            enlistResources(tx);                  
         }
         
         producer = sess.createProducer(destDest);
                          
         consumer.setMessageListener(new SourceListener());
         
         connSource.start();
         
         return true;
      }
      catch (Exception e)
      {
         log.warn("Failed to set up connections", e);
         
         //If this fails we should attempt to cleanup or we might end up in some weird state
         
         cleanup();
         
         return false;
      }
   }
   
   private void cleanup()
   {
      if (tx != null)
      {
         try
         {
            delistResources(tx);
         }
         catch (Throwable ignore)
         {
         } 
         try
         {
            //Terminate the tx
            tx.rollback();
         }
         catch (Throwable ignore)
         {
         } 
      }
      
      //Close the old objects
      try
      {
         connSource.close();
      }
      catch (Throwable ignore)
      {            
      }
      try
      {
         connDest.close();
      }
      catch (Throwable ignore)
      {            
      }
   }
   
   private void pause(long interval)
   {
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < failureRetryInterval)
      {
         try
         {
            Thread.sleep(failureRetryInterval);
         }
         catch (InterruptedException ex)
         {                  
         }
      }
   }
   
   private boolean setupJMSObjectsWithRetry()
   {
      if (trace) { log.trace("Setting up connections"); }
      
      int count = 0;
      
      while (true)
      {
         boolean ok = setupJMSObjects();
         
         if (ok)
         {
            return true;
         }
         
         count++;
         
         if (maxRetries != -1 && count == maxRetries)
         {
            break;
         }
         
         log.warn("Failed to set up connections, will retry after a pause of " + failureRetryInterval);
         
         pause(failureRetryInterval);
      }
      
      //If we get here then we exceed maxRetries
      return false;      
   }
    
   /*
    * If one of the JMS operations fail, then we try and lookup the connection factories, create
    * the connections and retry, up to a certain number of times
    */
   private void sendBatch() 
   {
      if (trace) { log.trace("Sending batch of " + messages.size() + " messages"); }
        
      synchronized (lock)
      {
         try
         {
            if (paused)
            {
               //Don't send now
               if (trace) { log.trace("Paused, so not sending now"); }
               
               return;            
            }
            
            if (qualityOfServiceMode == QOS_AT_MOST_ONCE)
            {
               //We ack before we send
               if (manualAck)
               {
                  //Ack on the last message
                  ((Message)messages.getLast()).acknowledge();       
               }
            }
            
            //Now send the message(s)   
               
            Iterator iter = messages.iterator();
            
            Message msg = null;
            
            while (iter.hasNext())
            {
               msg = (Message)iter.next();
               
               if (trace) { log.trace("Sending message " + msg); }
               
               producer.send(msg);
               
               if (trace) { log.trace("Sent message " + msg); }                    
            }
            
            if (qualityOfServiceMode == QOS_DUPLICATES_OK)
            {
               //We ack the source message(s) after sending
               
               if (manualAck)
               {               
                  //Ack on the last message
                  ((Message)messages.getLast()).acknowledge();
               }                  
            }
            
            //Now we commit the sending session if necessary
            if (manualCommit)
            {
               sessDest.commit();            
            }
            
            if (qualityOfServiceMode == QOS_ONCE_AND_ONLY_ONCE)
            {
               //Commit the JTA transaction and start another
                                       
               delistResources(tx);
                  
               if (trace) { log.trace("Committing JTA transaction"); }
               
               tx.commit();
               
               if (trace) { log.trace("Committed JTA transaction"); }
               
               tx = startTx();  
               
               enlistResources(tx);
            }
            
            //Clear the messages
            messages.clear();            
         }
         catch (Exception e)
         {
            log.warn("Failed to send + acknowledge batch, closing JMS objects", e);
            
            /*
             * If an Exception occurs in attempting to send / ack the batch, this might be due
             * to a network problem on either the source or destination connection.
             * If it was on the source connection then the server has probably NACKed the unacked
             * messages back to the destination anyway.
             * If the failure occurred during 2PC commit protocol then the participants may or may not
             * have reached the prepared state, if they do then the tx will commit at some time during
             * recovery.
             * 
             * So we can safely close the dead connections, without fear of stepping outside our
             * QoS guarantee.
             * 
             * 
             */
            
            //Clear the messages
            messages.clear();
                        
            cleanup();
            
            boolean ok = false;
            
            if (maxRetries > 0 || maxRetries == -1)
            {
               log.warn("Will try and re-set up connections after a pause of " + failureRetryInterval);
               
               pause(this.failureRetryInterval);
               
               //Now we try
               ok = setupJMSObjectsWithRetry();
            }
            
            if (!ok)
            {
               //We haven't managed to recreate connections or maxRetries = 0
               log.warn("Unable to set up connections, bridge will be stopped");
               
               try
               {                  
                  stop();
               }
               catch (Exception ignore)
               {                  
               }
            }
            
         }                                                      
      }
   }
   
   // Inner classes ---------------------------------------------------------------
   
   private class BatchTimeChecker implements Runnable
   {
      public void run()
      {
         if (trace) { log.trace(this + " running"); }
         
         synchronized (lock)
         {
            while (started)
            {
               long toWait = batchExpiryTime - System.currentTimeMillis();
               
               if (toWait <= 0)
               {
                  if (trace) { log.trace(this + " waited enough"); }
                  
                  if (!messages.isEmpty())
                  {
                     if (trace) { log.trace(this + " got some messages so sending batch"); }
                     
                     sendBatch();
                     
                     if (trace) { log.trace(this + " sent batch"); }
                  }
                  
                  batchExpiryTime = System.currentTimeMillis() + maxBatchTime;
               }
               else
               {                    
                  try
                  {
                     if (trace) { log.trace(this + " waiting for " + toWait); }
                     
                     lock.wait(toWait);
                     
                     if (trace) { log.trace(this + " woke up"); }
                  }
                  catch (InterruptedException e)
                  {
                     //Ignore
                     if (trace) { log.trace(this + " thread was interrupted"); }
                  }
                  
               }
            }        
         }
      }      
   }  
   
   private class SourceListener implements MessageListener
   {
      public void onMessage(Message msg)
      {
         synchronized (lock)
         {
            if (trace) { log.trace(this + " received message " + msg); }
            
            messages.add(msg);
            
            batchExpiryTime = System.currentTimeMillis() + maxBatchTime;            
            
            if (trace) { log.trace(this + " rescheduled batchExpiryTime to " + batchExpiryTime); }
            
            if (trace) { log.trace("max Batch Size is " + maxBatchSize); }
            
            if (maxBatchSize != -1 && messages.size() >= maxBatchSize)
            {
               if (trace) { log.trace(this + " maxBatchSize has been reached so sending batch"); }
               
               sendBatch();
               
               if (trace) { log.trace(this + " sent batch"); }
            }                        
         }
      }      
   }   
   
}

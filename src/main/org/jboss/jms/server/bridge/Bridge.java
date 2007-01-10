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

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.naming.InitialContext;
import javax.transaction.TransactionManager;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;
import org.jboss.tm.TransactionManagerLocator;

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
   
   static
   {
      log = Logger.getLogger(Bridge.class);
      
      trace = log.isTraceEnabled();
   }
         
   private Hashtable sourceJNDIProperties;
   
   private Hashtable destJNDIProperties;
   
   private String sourceConnectionFactoryLookup;
   
   private String destConnectionFactoryLookup;
   
   private String sourceDestinationLookup;
   
   private String destDestinationLookup;
   
   private String sourceUsername;
   
   private String sourcePassword;
   
   private String destUsername;
   
   private String destPassword;
   
   private TransactionManager tm;
   
   private String selector;
   
   private long failureRetryInterval;
   
   private boolean transactional;
   
   private boolean XA;
   
   private int maxBatchSize;
   
   private long maxBatchTime;
   
   private String subName;
   
   private String clientID;
   
   private boolean started;
   
   private List messages;
   
   private Object lock;
   
   //Needed since we use the 1.0.2 JMS API so we can work with 1.0.2 providers
   private boolean sourceIsTopic;
   
   private boolean destIsTopic;
   
   private Connection connSource; 
   
   private Connection connDest;
   
   private Session sessSource;
   
   private Session sessDest;
   
   private MessageConsumer consumer;
   
   private MessageProducer producer;
        
   private BatchTimeChecker timeChecker;
   
   private Thread checkerThread;
   
   private long batchExpiryTime;
   
   private boolean paused;         
   
   private InitialContext icSource;
   
   private InitialContext icDest;
   
         
   public Bridge(Hashtable sourceJNDIProperties, Hashtable destJNDIProperties,
                 String sourceConnectionFactoryLookup, String destConnectionFactoryLookup,   
                 String sourceDestinationLookup, String destDestinationLookup,
                 String sourceUsername, String sourcePassword,
                 String destUsername, String destPassword,
                 String selector, long failureRetryInterval, boolean transactional,
                 boolean XA, int maxBatchSize, long maxBatchTime,
                 String subName, String clientID,
                 boolean sourceIsTopic, boolean destIsTopic)
   {
      if (sourceJNDIProperties == null)
      {
         throw new IllegalArgumentException("sourceJNDIProperties cannot be null");
      }
      if (destJNDIProperties == null)
      {
         throw new IllegalArgumentException("destJNDIProperties cannot be null");
      }
      if (sourceConnectionFactoryLookup == null)
      {
         throw new IllegalArgumentException("sourceConnectionFactoryLookup cannot be null");
      }
      if (destConnectionFactoryLookup == null)
      {
         throw new IllegalArgumentException("destConnectionFactoryLookup cannot be null");
      }
      if (sourceDestinationLookup == null)
      {
         throw new IllegalArgumentException("sourceDestinationLookup cannot be null");
      }
      if (destDestinationLookup == null)
      {
         throw new IllegalArgumentException("destDestinationLookup cannot be null");
      }
      if (failureRetryInterval < 0 && failureRetryInterval != -1)
      {
         throw new IllegalArgumentException("failureRetryInterval must be > 0 or -1 to represent no retry");
      }
      if (maxBatchSize < 1 && maxBatchSize != -1)
      {
         throw new IllegalArgumentException("maxBatchSize must be >= 1 or -1 to represent unlimited batch size");
      }
      if (maxBatchTime < 1 && maxBatchTime != -1)
      {
         throw new IllegalArgumentException("maxBatchTime must be >= 1 or -1 to represent unlimited batch time");
      }
      if (maxBatchTime == -1 && maxBatchSize == -1)
      {
         throw new IllegalArgumentException("Cannot have unlimited batch size and unlimited batch time");
      }
      
      this.sourceJNDIProperties = sourceJNDIProperties;
      
      this.destJNDIProperties = destJNDIProperties; 
      
      this.sourceConnectionFactoryLookup = sourceConnectionFactoryLookup; 
      
      this.destConnectionFactoryLookup = destConnectionFactoryLookup;
      
      this.sourceDestinationLookup = sourceDestinationLookup;
      
      this.destDestinationLookup = destDestinationLookup;
      
      this.sourceUsername = sourceUsername;
      
      this.sourcePassword = sourcePassword;
      
      this.destUsername = destUsername;
      
      this.destPassword = destPassword;
      
      this.selector = selector;
      
      this.failureRetryInterval = failureRetryInterval;
      
      this.transactional = transactional;
      
      this.XA = XA;
      
      this.maxBatchSize = maxBatchSize;
      
      this.maxBatchTime = maxBatchTime;
         
      this.subName = subName;
      
      this.clientID = clientID;
      
      this.sourceIsTopic = sourceIsTopic;
      
      this.destIsTopic = destIsTopic;
      
      this.messages = new ArrayList(maxBatchSize);
      
      this.lock = new Object();
      
      if (trace)
      {
         log.trace("Created " + this);
      }
   }
   
   private TransactionManager getTm()
   {
      if (tm == null)
      {
         tm = TransactionManagerLocator.getInstance().locate();
         
         if (tm == null)
         {
            throw new IllegalStateException("Cannot locate a transaction manager");
         }
      }
      
      return tm;
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
      
      icSource = new InitialContext(sourceJNDIProperties);
      
      icDest = new InitialContext(destJNDIProperties);
      
      ConnectionFactory cfSource = (ConnectionFactory)icSource.lookup(sourceConnectionFactoryLookup);
      
      ConnectionFactory cfDest = (ConnectionFactory)icDest.lookup(destConnectionFactoryLookup);
      
      Destination sourceDest = (Destination)icSource.lookup(sourceDestinationLookup);
      
      Destination destDest = (Destination)icDest.lookup(destDestinationLookup);
      
      if (sourceUsername == null)
      {
         connSource = cfSource.createConnection();         
      }
      else
      {
         connSource = cfSource.createConnection(sourceUsername, sourcePassword);     
      }
      
      if (destUsername == null)
      {
         connDest = cfDest.createConnection();         
      }
      else
      {
         connDest = cfDest.createConnection(destUsername, destPassword);     
      }

      if (clientID != null)
      {
         connDest.setClientID(clientID);
      }
       
      //Note we use the JMS 1.0.2 API so we can interoperate with JMS providers that don't support
      //JMS 1.1
      if (sourceIsTopic)
      {         
         sessSource =
            ((TopicConnection)connSource).createTopicSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);
         
         if (subName == null)
         {
            //Non durable
            if (selector == null)
            {
               consumer = ((TopicSession)sessSource).createSubscriber((Topic)sourceDest);
            }
            else
            {
               consumer = ((TopicSession)sessSource).createSubscriber((Topic)sourceDest, selector, false);
            }
         }
         else
         {
            //Durable
            if (selector == null)
            {
               consumer = ((TopicSession)sessSource).createDurableSubscriber((Topic)sourceDest, subName);
            }
            else
            {
               consumer = ((TopicSession)sessSource).createDurableSubscriber((Topic)sourceDest, subName, selector, false);
            }
         }
      }
      else
      {
         sessSource =
            ((QueueConnection)connSource).createQueueSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.CLIENT_ACKNOWLEDGE);

         if (subName == null)
         {
            if (selector == null)
            {
               consumer = ((QueueSession)sessSource).createReceiver((Queue)sourceDest);
            }
            else
            {
               consumer = ((QueueSession)sessSource).createReceiver((Queue)sourceDest, selector);
            }
         }
         else
         {
            //Shouldn't specify sub name for source quuee
            throw new IllegalArgumentException("subName should not be specified if the source destination is a queue");
         }
      }
      
      if (destIsTopic)
      {
         sessDest =
            ((TopicConnection)connDest).createTopicSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
         
         producer = ((TopicSession)sessDest).createProducer((Topic)destDest);
      }
      else
      {
         sessDest =
            ((QueueConnection)connDest).createQueueSession(transactional, transactional ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);

         producer = ((QueueSession)sessDest).createSender((Queue)destDest);
         
      }
      
      consumer.setMessageListener(new SourceListener());
      
      connSource.start();

      if (maxBatchTime != -1)
      {
         timeChecker = new BatchTimeChecker();
         
         checkerThread = new Thread(timeChecker);
         
         batchExpiryTime = System.currentTimeMillis() + maxBatchTime;
         
         checkerThread.start();
      }
      
      if (trace) { log.trace("Started " + this); }
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
         checkerThread.join();
      }
      
      connSource.close();
      
      connDest.close();

      icSource.close();
      
      icDest.close();
      
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
          
   private void sendBatch()
   {
      if (trace) { log.trace("Sending batch of " + messages.size() + " messages"); }
      
      synchronized (lock)
      {
         if (paused)
         {
            //Don't send now
            if (trace) { log.trace("Paused, so not sending now"); }
            
            return;            
         }
         
         Iterator iter = messages.iterator();
         
         Message msg = null;
         
         while (iter.hasNext())
         {
            msg = (Message)iter.next();
            
            try
            {
               if (trace) { log.trace("Sending message " + msg); }
               
               producer.send(msg);
               
               if (trace) { log.trace("Sent message " + msg); }
            }
            catch (Throwable t)
            {
               //Failed to send - deal with retries
               t.printStackTrace();
            }                        
         }
         
         if (transactional)
         {
            try
            {
               if (trace) { log.trace("Committing local sending tx"); }
               
               sessDest.commit();
               
               if (trace) { log.trace("Committed local sending tx"); }
            }
            catch (Throwable t)
            {
               //Deal with this
               t.printStackTrace();
            }
         }
         
         messages.clear();
         
         if (transactional)
         {
            try
            {
               if (trace) { log.trace("Committing local consuming tx"); }
               
               sessSource.commit();
               
               if (trace) { log.trace("Committed local consuming tx"); }
            }
            catch (Throwable t)
            {
               //Deal with this
               t.printStackTrace();
            }
         }
         else
         {
            try
            {
               if (trace) { log.trace("Acknowledging session"); }
               
               msg.acknowledge();
               
               if (trace) { log.trace("Acknowledged session"); }
            }
            catch (Throwable t)
            {
               //Deal with this
               t.printStackTrace();
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
            
            if (messages.size() >= maxBatchSize)
            {
               if (trace) { log.trace(this + " maxBatchSizew has been reached so sending batch"); }
               
               sendBatch();
               
               if (trace) { log.trace(this + " sent batch"); }
            }                        
         }
      }      
   }   
   
}

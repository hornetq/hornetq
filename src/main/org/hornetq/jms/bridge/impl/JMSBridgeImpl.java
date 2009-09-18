/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */ 

package org.hornetq.jms.bridge.impl;

import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
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

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.jms.bridge.ConnectionFactoryFactory;
import org.hornetq.jms.bridge.DestinationFactory;
import org.hornetq.jms.bridge.JMSBridge;
import org.hornetq.jms.bridge.QualityOfServiceMode;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.client.HornetQSession;

/**
 * 
 * A JMSBridge
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision:4566 $</tt>
 *
 * $Id:JMSBridge.java 4566 2008-06-24 08:01:35Z jmesnil $
 *
 */
public class JMSBridgeImpl implements HornetQComponent, JMSBridge
{
   private static final Logger log;
   
   private static boolean trace;
   
   static
   {
      log = Logger.getLogger(JMSBridgeImpl.class);
      
      trace = log.isTraceEnabled();
   }

   private static final int TEN_YEARS = 60 * 60 * 24 * 365 * 10; // in ms

   private final Object lock = new Object();
   
   private String sourceUsername;
   
   private String sourcePassword;
   
   private String targetUsername;
   
   private String targetPassword;
   
   private TransactionManager tm;
   
   private String selector;
   
   private long failureRetryInterval;
   
   private int maxRetries;
   
   private QualityOfServiceMode qualityOfServiceMode;
   
   private int maxBatchSize;
   
   private long maxBatchTime;
   
   private String subName;
   
   private String clientID;
   
   private volatile boolean addMessageIDInHeader;
   
   private boolean started;
   
   private LinkedList<Message> messages;
   
   private ConnectionFactoryFactory sourceCff;
   
   private ConnectionFactoryFactory targetCff;
   
   private DestinationFactory sourceDestinationFactory;
   
   private DestinationFactory targetDestinationFactory;
   
   private Connection sourceConn; 
   
   private Connection targetConn;
   
   private Destination sourceDestination;
   
   private Destination targetDestination;
   
   private Session sourceSession;
   
   private Session targetSession;
   
   private MessageConsumer sourceConsumer;
   
   private MessageProducer targetProducer;
        
   private BatchTimeChecker timeChecker;
   
   private Thread checkerThread;
   
   private Thread sourceReceiver;

   private long batchExpiryTime;
   
   private boolean paused;         
   
   private Transaction tx;  
   
   private boolean failed;
   
   private int forwardMode;

   private String transactionManagerLocatorClass = "org.hornetq.integration.jboss.tm.JBoss5TransactionManagerLocator";

   private String transactionManagerLocatorMethod = "getTm";
   
   private static final int FORWARD_MODE_XA = 0;
   
   private static final int FORWARD_MODE_LOCALTX = 1;
   
   private static final int FORWARD_MODE_NONTX = 2;
   
   /*
    * Constructor for MBean
    */
   public JMSBridgeImpl()
   {      
      this.messages = new LinkedList<Message>();      
   }
   
   public JMSBridgeImpl(ConnectionFactoryFactory sourceCff, ConnectionFactoryFactory targetCff,
                 DestinationFactory sourceDestinationFactory, DestinationFactory targetDestinationFactory,         
                 String sourceUsername, String sourcePassword,
                 String targetUsername, String targetPassword,
                 String selector, long failureRetryInterval,
                 int maxRetries,
                 QualityOfServiceMode qosMode,
                 int maxBatchSize, long maxBatchTime,
                 String subName, String clientID,
                 boolean addMessageIDInHeader)
   {            
      this();
      
      this.sourceCff = sourceCff;
      
      this.targetCff = targetCff;
      
      this.sourceDestinationFactory = sourceDestinationFactory;
      
      this.targetDestinationFactory = targetDestinationFactory;
      
      this.sourceUsername = sourceUsername;
      
      this.sourcePassword = sourcePassword;
      
      this.targetUsername = targetUsername;
      
      this.targetPassword = targetPassword;
      
      this.selector = selector;
      
      this.failureRetryInterval = failureRetryInterval;
      
      this.maxRetries = maxRetries;
      
      this.qualityOfServiceMode = qosMode;
      
      this.maxBatchSize = maxBatchSize;
      
      this.maxBatchTime = maxBatchTime;
      
      this.subName = subName;
      
      this.clientID = clientID;
      
      this.addMessageIDInHeader = addMessageIDInHeader;
              
      checkParams();
      
      if (trace)
      {
         log.trace("Created " + this);
      }
   }
         
   // HornetQComponent overrides --------------------------------------------------
        
   public synchronized void start() throws Exception
   {      
      if (started)
      {
         log.warn("Attempt to start, but is already started");
         return;
      }
      
      if (trace) { log.trace("Starting " + this); }         
      
      checkParams();
      
      TransactionManager tm = getTm();
      
      //There may already be a JTA transaction associated to the thread
      
      boolean ok;
      
      Transaction toResume = null;
      try
      {
         toResume = tm.suspend();
         
         ok = setupJMSObjects();
      }
      finally
      {
         if (toResume != null)
         {
            tm.resume(toResume);
         }
      }
      
      if (ok)
      {         
         //start the source connection
         
         sourceConn.start();
         
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
         
         sourceReceiver = new SourceReceiver();
         sourceReceiver.start();
         
         if (trace) { log.trace("Started " + this); }
      }
      else
      {
         log.warn("Failed to start bridge");
         handleFailureOnStartup();
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
         
         if (sourceReceiver != null)
         {
            sourceReceiver.interrupt();
         }
      }
            
      //This must be outside sync block
      if (checkerThread != null)
      {  
         if (trace) { log.trace("Waiting for checker thread to finish");}
         
         checkerThread.join();
         
         if (trace) { log.trace("Checker thread has finished"); }
      }
      
      //This must be outside sync block
      if (sourceReceiver != null)
      {  
         if (trace) { log.trace("Waiting for source receiver thread to finish");}
         
         sourceReceiver.join();
         
         if (trace) { log.trace("Source receiver thread has finished"); }
      }
      
      if (tx != null)
      {
         //Terminate any transaction
         if (trace) { log.trace("Rolling back remaining tx"); }
         
         try
         {
            tx.rollback();
         }
         catch (Exception ignore)
         {
            if (trace) { log.trace("Failed to rollback", ignore); }
         }
         
         if (trace) { log.trace("Rolled back remaining tx"); }
      }
      
      try
      {
         sourceConn.close();
      }
      catch (Exception ignore)
      {
         if (trace) { log.trace("Failed to close source conn", ignore); }
      }
      
      if (targetConn != null)
      {
         try
         {
            targetConn.close();
         }
         catch (Exception ignore)
         {
            if (trace) { log.trace("Failed to close target conn", ignore); }
         }
      }
            
      if (trace) { log.trace("Stopped " + this); }
   }
   
   public synchronized boolean isStarted()
   {
      return started;
   }

   // JMSBridge implementation ------------------------------------------------------------

   public synchronized void pause() throws Exception
   {
      if (trace) { log.trace("Pausing " + this); }
      
      synchronized (lock)
      {
         paused = true;
         
         sourceConn.stop();
      }
      
      if (trace) { log.trace("Paused " + this); }
   }
   
   public synchronized void resume() throws Exception
   {
      if (trace) { log.trace("Resuming " + this); }
      
      synchronized (lock)
      {
         paused = false;
         
         sourceConn.start();
      }
      
      if (trace) { log.trace("Resumed " + this); }
   }
      
   public DestinationFactory getSourceDestinationFactory()
   {
   	return sourceDestinationFactory;
   }

   public void setSourceDestinationFactory(DestinationFactory dest)
   {
      checkBridgeNotStarted();
   	checkNotNull(dest, "TargetDestinationFactory");
   	
   	sourceDestinationFactory = dest;
   }
   
   public DestinationFactory getTargetDestinationFactory()
   {
   	return targetDestinationFactory;
   }

   public void setTargetDestinationFactory(DestinationFactory dest)
   {
      checkBridgeNotStarted();
      checkNotNull(dest, "TargetDestinationFactory");
      
   	targetDestinationFactory = dest;
   }
   
   public String getSourceUsername()
   {
      return sourceUsername;
   }
   
   public synchronized void setSourceUsername(String name)
   {
      checkBridgeNotStarted();
      
      sourceUsername = name;
   }
   
   public synchronized String getSourcePassword()
   {
      return sourcePassword;
   }
   
   public synchronized void setSourcePassword(String pwd)
   {
      checkBridgeNotStarted();
      
      sourcePassword = pwd;
   }
      
   public synchronized String getTargetUsername()
   {
      return targetUsername;
   }
   
   public synchronized void setTargetUsername(String name)
   {
      checkBridgeNotStarted();
      
      this.targetUsername = name;
   }
   
   public synchronized String getTargetPassword()
   {
      return this.targetPassword;
   }
   
   public synchronized void setTargetPassword(String pwd)
   {
      checkBridgeNotStarted();
      
      this.targetPassword = pwd;
   }

   public synchronized String getSelector()
   {
      return selector;
   }
   
   public synchronized void setSelector(String selector)
   {
      checkBridgeNotStarted();
      
      this.selector = selector;
   }
   
   public synchronized long getFailureRetryInterval()
   {
      return failureRetryInterval;
   }
   
   public synchronized void setFailureRetryInterval(long interval)
   {
      checkBridgeNotStarted();
      checkValidValue(interval, "FailureRetryInterval");
      
      this.failureRetryInterval = interval;
   }  
   
   public synchronized int getMaxRetries()
   {
      return maxRetries;
   }
   
   public synchronized void setMaxRetries(int retries)
   {
      checkBridgeNotStarted();
      checkValidValue(retries, "MaxRetries");
      
      this.maxRetries = retries;
   }
      
   public synchronized QualityOfServiceMode getQualityOfServiceMode()
   {
      return qualityOfServiceMode;
   }
   
   public synchronized void setQualityOfServiceMode(QualityOfServiceMode mode)
   {
      checkBridgeNotStarted();
      checkNotNull(mode, "QualityOfServiceMode");
      
      qualityOfServiceMode = mode;
   }   
   
   public synchronized int getMaxBatchSize()
   {
      return maxBatchSize;
   }
   
   public synchronized void setMaxBatchSize(int size)
   {
      checkBridgeNotStarted();
      checkMaxBatchSize(size);
      
      maxBatchSize = size;
   }
   
   public synchronized long getMaxBatchTime()
   {
      return maxBatchTime;
   }
   
   public synchronized void setMaxBatchTime(long time)
   {
      checkBridgeNotStarted();
      checkValidValue(time, "MaxBatchTime");
      
      maxBatchTime = time;
   }
      
   public synchronized String getSubscriptionName()
   {
      return this.subName;
   }
   
   public synchronized void setSubscriptionName(String subname)
   {
      checkBridgeNotStarted();
      this.subName = subname; 
   }
      
   public synchronized String getClientID()
   {
      return clientID;
   }
   
   public synchronized void setClientID(String clientID)
   {
      checkBridgeNotStarted();
      
      this.clientID = clientID; 
   }

   public String getTransactionManagerLocatorClass()
   {
      return transactionManagerLocatorClass;
   }

   public void setTransactionManagerLocatorClass(String transactionManagerLocatorClass)
   {
      checkBridgeNotStarted();
      this.transactionManagerLocatorClass = transactionManagerLocatorClass;
   }

   public String getTransactionManagerLocatorMethod()
   {
      return transactionManagerLocatorMethod;
   }

   public void setTransactionManagerLocatorMethod(String transactionManagerLocatorMethod)
   {
      this.transactionManagerLocatorMethod = transactionManagerLocatorMethod;
   }

   public boolean isAddMessageIDInHeader()
   {
   	return this.addMessageIDInHeader;
   }
   
   public void setAddMessageIDInHeader(boolean value)
   {
   	this.addMessageIDInHeader = value;
   }
      
   public synchronized boolean isPaused()
   {
      return paused;
   }
   
   public synchronized boolean isFailed()
   {
      return failed;
   }

   public synchronized void setSourceConnectionFactoryFactory(ConnectionFactoryFactory cff)
   {
      checkBridgeNotStarted();
      checkNotNull(cff, "SourceConnectionFactoryFactory");
      
      this.sourceCff = cff;
   }
   
   public synchronized void setTargetConnectionFactoryFactory(ConnectionFactoryFactory cff)
   {
      checkBridgeNotStarted();
      checkNotNull(cff, "TargetConnectionFactoryFactory");

      this.targetCff = cff;
   }
   
   public void setTransactionManager(TransactionManager tm)
   {
      this.tm = tm;
   }

   // Public ---------------------------------------------------------------------------
   
   // Private -------------------------------------------------------------------

   private void checkParams()
   {
      checkNotNull(sourceCff, "sourceCff");
      checkNotNull(targetCff, "targetCff");
      checkNotNull(sourceDestinationFactory, "sourceDestinationFactory");
      checkNotNull(targetDestinationFactory, "targetDestinationFactory");
      checkValidValue(failureRetryInterval, "failureRetryInterval");
      checkValidValue(maxRetries, "maxRetries");
      if (failureRetryInterval == -1 && maxRetries > 0)
      {
         throw new IllegalArgumentException("If failureRetryInterval == -1 maxRetries must be set to -1");
      }
      checkMaxBatchSize(maxBatchSize);
      checkValidValue(maxBatchTime, "maxBatchTime");
      checkNotNull(qualityOfServiceMode, "qualityOfServiceMode");
   }

   /**
    * Check the object is not null
    * 
    * @throws IllegalArgumentException if the object is null
    */
   private static void checkNotNull(Object obj, String name)
   {
      if (obj == null)
      {
         throw new IllegalArgumentException(name + " cannot be null");
      }
   }
   
   /**
    * Check the bridge is not started
    * 
    * @throws IllegalStateException if the bridge is started
    */
   private void checkBridgeNotStarted()
   {
      if (started)
      {
         throw new IllegalStateException("Cannot set bridge attributes while it is started");
      }
   }
   
   /**
    * Check that value is either equals to -1 or greater than 0
    * 
    * @throws IllegalArgumentException if the value is not valid
    */
   private static void checkValidValue(long value, String name)
   {
      if (!(value == -1 || value > 0))
      {
         throw new IllegalArgumentException(name + " must be > 0 or -1");
      }
   }  
   
   private static void checkMaxBatchSize(int size)
   {
      if (!(size >= 1))
      {
         throw new IllegalArgumentException("maxBatchSize must be >= 1");
      }
   }

   private void enlistResources(Transaction tx) throws Exception
   {
      if (trace) { log.trace("Enlisting resources in tx"); }
      
      XAResource resSource = ((XASession)sourceSession).getXAResource();
      
      tx.enlistResource(resSource);
      
      XAResource resDest = ((XASession)targetSession).getXAResource();
      
      tx.enlistResource(resDest);
      
      if (trace) { log.trace("Enlisted resources in tx"); }
   }
   
   private void delistResources(Transaction tx)
   {
      if (trace) { log.trace("Delisting resources from tx"); }
      
      XAResource resSource = ((XASession)sourceSession).getXAResource();

      try
      {
         tx.delistResource(resSource, XAResource.TMSUCCESS);
      }
      catch (Exception e)
      {
         if (trace) { log.trace("Failed to delist source resource", e); }
      }

      
      XAResource resDest = ((XASession)targetSession).getXAResource();
      
      try
      {
         tx.delistResource(resDest, XAResource.TMSUCCESS);
      }
      catch (Exception e)
      {
         if (trace) { log.trace("Failed to delist target resource", e); }
      }
      
      if (trace) { log.trace("Delisted resources from tx"); }
   }
   
   private Transaction startTx() throws Exception
   {
      if (trace) { log.trace("Starting JTA transaction"); }
      
      TransactionManager tm = getTm();
      
      //Set timeout to a large value since we do not want to time out while waiting for messages
      //to arrive - 10 years should be enough
      tm.setTransactionTimeout(TEN_YEARS);
      
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
         try
         {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            Class aClass = loader.loadClass(transactionManagerLocatorClass);
            Object o = aClass.newInstance();
            Method m = aClass.getMethod(transactionManagerLocatorMethod);
            tm = (TransactionManager) m.invoke(o);
         }
         catch (Exception e)
         {
            throw new IllegalStateException("unable to create TransactionManager from " + transactionManagerLocatorClass
                                            + "." + transactionManagerLocatorMethod, e);
         }

         if (tm == null)
         {
            throw new IllegalStateException("Cannot locate a transaction manager");
         }         
      }
      
      return tm;
   }
         
   private Connection createConnection(String username, String password, ConnectionFactoryFactory cff)
      throws Exception
   {
      Connection conn;
      
      ConnectionFactory cf = cff.createConnectionFactory();
      
      if (qualityOfServiceMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE &&
          !(cf instanceof XAConnectionFactory))
      {
         throw new IllegalArgumentException("Connection factory must be XAConnectionFactory");
      }
      
      if (username == null)
      {
         if (qualityOfServiceMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE)
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
         if (qualityOfServiceMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE)
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
    
   /*
    * Source and target on same server
    * --------------------------------
    * If the source and target destinations are on the same server (same resource manager) then,
    * in order to get ONCE_AND_ONLY_ONCE, we simply need to consuming and send in a single
    * local JMS transaction.
    * 
    * We actually use a single local transacted session for the other QoS modes too since this
    * is more performant than using DUPS_OK_ACKNOWLEDGE or AUTO_ACKNOWLEDGE session ack modes, so effectively
    * the QoS is upgraded.
    * 
    * Source and target on different server
    * -------------------------------------
    * If the source and target destinations are on a different servers (different resource managers) then:
    * 
    * If desired QoS is ONCE_AND_ONLY_ONCE, then we start a JTA transaction and enlist the consuming and sending
    * XAResources in that.
    * 
    * If desired QoS is DUPLICATES_OK then, we use CLIENT_ACKNOWLEDGE for the consuming session and
    * AUTO_ACKNOWLEDGE (this is ignored) for the sending session if the maxBatchSize == 1, otherwise we
    * use a local transacted session for the sending session where maxBatchSize > 1, since this is more performant
    * When bridging a batch, we make sure to manually acknowledge the consuming session, if it is CLIENT_ACKNOWLEDGE
    * *after* the batch has been sent
    * 
    * If desired QoS is AT_MOST_ONCE then, if maxBatchSize == 1, we use AUTO_ACKNOWLEDGE for the consuming session,
    * and AUTO_ACKNOWLEDGE for the sending session.
    * If maxBatchSize > 1, we use CLIENT_ACKNOWLEDGE for the consuming session and a local transacted session for the
    * sending session.
    * 
    * When bridging a batch, we make sure to manually acknowledge the consuming session, if it is CLIENT_ACKNOWLEDGE
    * *before* the batch has been sent
    * 
    */
   private boolean setupJMSObjects()
   {
      try
      {  
         if (sourceCff == targetCff)
         {
            //Source and target destinations are on the server - we can get once and only once
            //just using a local transacted session
         	//everything becomes once and only once
         	
         	forwardMode = FORWARD_MODE_LOCALTX;
         }
         else
         {
         	//Different servers
         	if (qualityOfServiceMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE)
         	{
         		//Use XA
         		
         		forwardMode = FORWARD_MODE_XA;
         	}
         	else
         	{
         		forwardMode = FORWARD_MODE_NONTX;
         	}
         }
         
      	//Lookup the destinations
      	sourceDestination = sourceDestinationFactory.createDestination();
      	
      	targetDestination = targetDestinationFactory.createDestination();      	      
         
         sourceConn = createConnection(sourceUsername, sourcePassword, sourceCff);
         
         if (forwardMode != FORWARD_MODE_LOCALTX)
         {
            targetConn = createConnection(targetUsername, targetPassword, targetCff);
            
            targetConn.setExceptionListener(new BridgeExceptionListener());            
         }
                  
         if (clientID != null)
         {
            sourceConn.setClientID(clientID);
         }
         
         sourceConn.setExceptionListener(new BridgeExceptionListener());         
          
         Session sess;
         
         if (forwardMode == FORWARD_MODE_LOCALTX)
         {
            //We simply use a single local transacted session for consuming and sending      
            
            sourceSession = sourceConn.createSession(true, Session.SESSION_TRANSACTED);
            
            sess = sourceSession;
         }
         else
         {
            if (forwardMode == FORWARD_MODE_XA)
            {
               //Create an XASession for consuming from the source
               if (trace) { log.trace("Creating XA source session"); }
               
               sourceSession = ((XAConnection)sourceConn).createXASession();
               
               sess = ((XASession)sourceSession).getSession();
            }
            else
            {
               if (trace) { log.trace("Creating non XA source session"); }
               
               //Create a standard session for consuming from the source
               
               //We use ack mode client ack
                              
               sourceSession = sourceConn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
               
               sess = sourceSession;
            }
         }
         
         if (forwardMode == FORWARD_MODE_XA && sourceSession instanceof HornetQSession)
         {
         	HornetQSession jsession = (HornetQSession)sourceSession;

         	ClientSession clientSession = jsession.getCoreSession();
            
         	//clientSession.setTreatAsNonTransactedWhenNotEnlisted(false);
         }
            
         if (subName == null)
         {
            if (selector == null)
            {
               sourceConsumer = sess.createConsumer(sourceDestination);
            }
            else
            {
               sourceConsumer = sess.createConsumer(sourceDestination, selector, false);
            }
         }
         else
         {
            //Durable subscription
            if (selector == null)
            {
               sourceConsumer = sess.createDurableSubscriber((Topic)sourceDestination, subName);
            }
            else
            {
               sourceConsumer = sess.createDurableSubscriber((Topic)sourceDestination, subName, selector, false);
            }
         }
         
         //Now the sending session
         
         
         if (forwardMode != FORWARD_MODE_LOCALTX)
         {            
            if (forwardMode == FORWARD_MODE_XA)
            {
               if (trace) { log.trace("Creating XA dest session"); }
               
               //Create an XA sesion for sending to the destination
               
               targetSession = ((XAConnection)targetConn).createXASession();
               
               sess = ((XASession)targetSession).getSession();
            }
            else
            {
               if (trace) { log.trace("Creating non XA dest session"); }
               
               //Create a standard session for sending to the target
                                             
               //If batch size > 1 we use a transacted session since is more efficient
               
               boolean transacted = maxBatchSize > 1;
               
               targetSession = targetConn.createSession(transacted, transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
               
               sess = targetSession;
            }       
         }
         
         if (forwardMode == FORWARD_MODE_XA)
         {
            if (trace) { log.trace("Starting JTA transaction"); }
            
            tx = startTx();
            
            enlistResources(tx);                  
         }
         
         targetProducer = sess.createProducer(null);
         
         return true;
      }
      catch (Exception e)
      {
         log.warn("Failed to set up bridge connections", e);
         
         //If this fails we should attempt to cleanup or we might end up in some weird state
         
         cleanup();
         
         return false;
      }
   }
   
   private void cleanup()
   {
      //Stop the source connection
      try
      {
         sourceConn.stop();
      }
      catch (Throwable ignore)
      {            
         if (trace) { log.trace("Failed to stop source connection", ignore); }
      }
      
      if (tx != null)
      {
         try
         {
            delistResources(tx);
         }
         catch (Throwable ignore)
         {
         	if (trace) { log.trace("Failed to delist resources", ignore); }
         } 
         try
         {
            //Terminate the tx
            tx.rollback();
         }
         catch (Throwable ignore)
         {
         	if (trace) { log.trace("Failed to rollback", ignore); }
         } 
      }
      
      //Close the old objects
      try
      {
         sourceConn.close();
      }
      catch (Throwable ignore)
      {            
         if (trace) { log.trace("Failed to close source connection", ignore); }
      }
      try
      {
         if (targetConn != null)
         {
            targetConn.close();
         }
      }
      catch (Throwable ignore)
      {    
         if (trace) { log.trace("Failed to close target connection", ignore); }
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
         
         log.warn("Failed to set up connections, will retry after a pause of " + failureRetryInterval + " ms");
         
         pause(failureRetryInterval);
      }
      
      //If we get here then we exceed maxRetries
      return false;      
   }
      
   
   private void sendBatch() 
   {
      if (trace) { log.trace("Sending batch of " + messages.size() + " messages"); }
        
      if (paused)
      {
         //Don't send now
         if (trace) { log.trace("Paused, so not sending now"); }
         
         return;            
      }
      
      if (forwardMode == FORWARD_MODE_LOCALTX)
      {
         sendBatchLocalTx();
      }
      else if (forwardMode == FORWARD_MODE_XA)        
      {
         sendBatchXA();
      }
      else
      {
         sendBatchNonTransacted();
      }
   }
   
   private void sendBatchNonTransacted()
   {
   	try
      {         
   		if (qualityOfServiceMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE)
   		{
   			//We client ack before sending
   			
            if (trace) { log.trace("Client acking source session"); }
               			
            ((Message)messages.getLast()).acknowledge();
            
            if (trace) { log.trace("Client acked source session"); }            
   		}
   		   		
         sendMessages();
         
         if (maxBatchSize > 1)
         {
         	//The sending session is transacted - we need to commit it
         	
            if (trace) { log.trace("Committing target session"); }
                     	
         	targetSession.commit();
         	
            if (trace) { log.trace("Committed source session"); }            
         }
         
         if (qualityOfServiceMode == QualityOfServiceMode.DUPLICATES_OK)
   		{
   			//We client ack after sending
         	
         	//Note we could actually use Session.DUPS_OK_ACKNOWLEDGE here
         	//For a slightly less strong delivery guarantee
   		
            if (trace) { log.trace("Client acking source session"); }
               			
            messages.getLast().acknowledge();
            
            if (trace) { log.trace("Client acked source session"); }            
   		}
                                         
         //Clear the messages
         messages.clear();            
      }
      catch (Exception e)
      {
         log.warn("Failed to send + acknowledge batch, closing JMS objects", e);
      
         handleFailureOnSend();                                                 
      }	
   }
   
   private void sendBatchXA()
   {
   	try
      {         
         sendMessages();
         
         //Commit the JTA transaction and start another
                                 
         delistResources(tx);
            
         if (trace) { log.trace("Committing JTA transaction"); }
         
         tx.commit();

         if (trace) { log.trace("Committed JTA transaction"); }
         
         tx = startTx();  
         
         enlistResources(tx);
                  
         //Clear the messages
         messages.clear();            
      }
      catch (Exception e)
      {
         log.warn("Failed to send + acknowledge batch, closing JMS objects", e);
      
         handleFailureOnSend();                                                 
      }
   }
   
   private void sendBatchLocalTx()
   {
   	try
      {         
         sendMessages();
                     
         if (trace) { log.trace("Committing source session"); }
         
         sourceSession.commit();
         
         if (trace) { log.trace("Committed source session"); }     
         
         //Clear the messages
         messages.clear();           
      }
      catch (Exception e)
      {
         log.warn("Failed to send + acknowledge batch, closing JMS objects", e);
      
         handleFailureOnSend();                                                 
      }
   }
   
   private void sendMessages() throws Exception
   {
      Iterator iter = messages.iterator();
      
      Message msg = null;
      
      while (iter.hasNext())
      {
      	msg = (Message)iter.next();
      	
      	if (addMessageIDInHeader)
         {
         	addMessageIDInHeader(msg);            	
         }
         
         if (trace) { log.trace("Sending message " + msg); }
         
         //Make sure the correct time to live gets propagated
         
         long timeToLive = msg.getJMSExpiration();
         
   		if (timeToLive != 0)
   		{
   			timeToLive -=  System.currentTimeMillis();
   			
   			if (timeToLive <= 0)
   			{
   				timeToLive = 1; //Should have already expired - set to 1 so it expires when it is consumed or delivered
   			}
   		}
         
   		targetProducer.send(targetDestination, msg, msg.getJMSDeliveryMode(), msg.getJMSPriority(), timeToLive);
   		
         if (trace) { log.trace("Sent message " + msg); }     
      }
   }
   
   private void handleFailureOnSend()
   {
      handleFailure(new FailureHandler());
   }
   
   private void handleFailureOnStartup()
   {
      handleFailure(new StartupFailureHandler());
   }
   
   private void handleFailure(Runnable failureHandler)
   {
      failed = true;

      //Failure must be handled on a separate thread to the calling thread (either onMessage or start).
      //In the case of onMessage we can't close the connection from inside the onMessage method
      //since it will block waiting for onMessage to complete. In the case of start we want to return
      //from the call before the connections are reestablished so that the caller is not blocked unnecessarily.
      Thread t = new Thread(failureHandler);
      
      t.start();         
   }
   
   private void addMessageIDInHeader(Message msg) throws Exception
   {
   	//We concatenate the old message id as a header in the message
   	//This allows the target to then use this as the JMSCorrelationID of any response message
   	//thus enabling a distributed request-response pattern.
   	//Each bridge (if there are more than one) in the chain can concatenate the message id
   	//So in the case of multiple bridges having routed the message this can be used in a multi-hop
   	//distributed request/response
   	if (trace) { log.trace("Adding old message id in Message header"); }
   	
   	copyProperties(msg);

   	String val = null;
   	
   	val = msg.getStringProperty(HornetQMessage.JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST);
   	
   	if (val == null)
   	{
   		val = msg.getJMSMessageID();
   	}
   	else
   	{
   		StringBuffer sb = new StringBuffer(val);
   		
   		sb.append(",").append(msg.getJMSMessageID());
   		
   		val = sb.toString();
   	}
   	
   	msg.setStringProperty(HornetQMessage.JBOSS_MESSAGING_BRIDGE_MESSAGE_ID_LIST, val);            	   
   }

   
   /*
    * JMS does not let you add a property on received message without first
    * calling clearProperties, so we need to save and re-add all the old properties so we
    * don't lose them!!
    */
   private static void copyProperties(Message msg) throws JMSException
   {   	
   	Enumeration en = msg.getPropertyNames();
   	
   	Map<String, Object> oldProps = null;
   	
   	while (en.hasMoreElements())
   	{
   		String propName = (String)en.nextElement();
   		
   		if (oldProps == null)
   		{
   			oldProps = new HashMap<String, Object>();
   		}
   		
   		oldProps.put(propName, msg.getObjectProperty(propName));
   	}
   	            	
   	msg.clearProperties();
   	
   	if (oldProps != null)
   	{
   		Iterator oldPropsIter = oldProps.entrySet().iterator();
   		
   		while (oldPropsIter.hasNext())
   		{
   			Map.Entry entry = (Map.Entry)oldPropsIter.next();

   			String propName = (String)entry.getKey();

   			msg.setObjectProperty(propName, entry.getValue());   			
   		}
   	}
   }
   
   // Inner classes ---------------------------------------------------------------
   

   /**
    * We use a Thread which polls the sourceDestination instead of a MessageListener
    * to ensure that message delivery does not happen concurrently with
    * transaction enlistment of the XAResource (see HORNETQ-27)
    *
    */
   private final class SourceReceiver extends Thread
   {
      @Override
      public void run()
      {
         while(started)
         {
            synchronized (lock)
            {
               if (paused || failed)
               {
                  try
                  {
                     lock.wait(500);
                  }
                  catch (InterruptedException e)
                  {
                     if (trace) { log.trace(this + " thread was interrupted"); }
                  }
                  continue;
               }

               Message msg = null;
               try
               {
                  msg = sourceConsumer.receive(1000);
               }
               catch (JMSException jmse)
               {
                  if (trace) { log.trace(this + " exception while receiving a message", jmse); }
               }

               if (msg == null)
               {
                  try
                  {
                     lock.wait(500);
                  }
                  catch (InterruptedException e)
                  {
                     if (trace) { log.trace(this + " thread was interrupted"); }
                  }
                  continue;
               }

               if (trace) { log.trace(this + " received message " + msg); }

               messages.add(msg);

               batchExpiryTime = System.currentTimeMillis() + maxBatchTime;            

               if (trace) { log.trace(this + " rescheduled batchExpiryTime to " + batchExpiryTime); }

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

   private class FailureHandler implements Runnable
   {
      /**
       * Start the source connection - note the source connection must not be started before
       * otherwise messages will be received and ignored
       */
      protected void startSourceConnection()
      {
         try
         {
            sourceConn.start();
         }
         catch (JMSException e)
         {
            log.error("Failed to start source connection", e);
         }
      }

      protected void succeeded()
      {
         log.info("Succeeded in reconnecting to servers");
         
         synchronized (lock)
         {
            failed = false;

            startSourceConnection();
         }
      }
      
      protected void failed()
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

      public void run()
      {
      	if (trace) { log.trace("Failure handler running"); }
      	
         // Clear the messages
         messages.clear();

         cleanup();
         
         boolean ok = false;
         
         if (maxRetries > 0 || maxRetries == -1)
         {
            log.warn("Will retry after a pause of " + failureRetryInterval + " ms");
            
            pause(failureRetryInterval);
            
            //Now we try
            ok = setupJMSObjectsWithRetry();
         }
         
         if (!ok)
         {
            failed();
         }
         else
         {
            succeeded();
         }    
      }
   }
   
   private class StartupFailureHandler extends FailureHandler
   {
      protected void failed()
      {
         // Don't call super
         log.warn("Unable to set up connections, bridge will not be started");
      }
      
      protected void succeeded()
      {
         // Don't call super - a bit ugly in this case but better than taking the lock twice.
         log.info("Succeeded in connecting to servers");
         
         synchronized (lock)
         {
            failed = false;
            started = true;
            
            //Start the source connection - note the source connection must not be started before
            //otherwise messages will be received and ignored
            
            try
            {
               sourceConn.start();
            }
            catch (JMSException e)
            {
               log.error("Failed to start source connection", e);
            }
         }
      }
   }
   
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
                  
                  synchronized (lock)
                  {              
                     if (!failed && !messages.isEmpty())
                     {
                        if (trace) { log.trace(this + " got some messages so sending batch"); }
                        
                        sendBatch();                     
                        
                        if (trace) { log.trace(this + " sent batch"); }
                     }
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
   
   private class BridgeExceptionListener implements ExceptionListener
   {
		public void onException(JMSException e)
		{
			log.warn("Detected failure on bridge connection");
			
			synchronized (lock)
			{
				if (failed)
				{
					//The failure has already been detected and is being handled
					if (trace) { log.trace("Failure recovery already in progress"); }
				}
				else
			   {				
					handleFailure(new FailureHandler());
			   }
			}
		}   	
   }   
}

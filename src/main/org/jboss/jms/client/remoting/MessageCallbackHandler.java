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
package org.jboss.jms.client.remoting;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.tx.AckInfo;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.Future;
import org.jboss.remoting.callback.HandleCallbackException;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox/a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageCallbackHandler
{
   // Constants -----------------------------------------------------
   
   private static final Logger log;
   
   // Static --------------------------------------------------------
   
   private static boolean trace;
   
   static
   {
      log = Logger.getLogger(MessageCallbackHandler.class);
      trace = log.isTraceEnabled();
   }
     
   //Hardcoded for now
   private static final int MAX_REDELIVERIES = 10;
      
   public static void callOnMessage(ConsumerDelegate cons,
                                    SessionDelegate sess,
                                    MessageListener listener,
                                    int consumerID,
                                    boolean isConnectionConsumer,
                                    MessageProxy m,
                                    int ackMode)
      throws JMSException
   {
      preDeliver(sess, consumerID, m, isConnectionConsumer);  
                  
      int tries = 0;
      
      while (true)
      {
         try
         {      
            listener.onMessage(m); 
            
            break;
         }
         catch (RuntimeException e)
         {
            long id = m.getMessage().getMessageID();
   
            log.error("RuntimeException was thrown from onMessage, " + id + " will be redelivered", e);
            
            // See JMS 1.1 spec 4.5.2
   
            if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE)
            {
               //We redeliver at certain number of times
               if (tries < MAX_REDELIVERIES)
               {
                  m.setJMSRedelivered(true);
                  
                  //TODO delivery count although optional should be global
                  //so we need to send it back to the server
                  //but this has performance hit so perhaps we just don't support it?
                  m.incDeliveryCount();
                  
                  tries++;
               }
               else
               {
                  log.error("Max redeliveries has occurred for message: " + m.getJMSMessageID());
                  
                  //TODO - Send to DLQ
                  
                  break;
               }
            }
            else
            {
               // Session is either transacted or CLIENT_ACKNOWLEDGE
               // We just deliver next message
               if (trace) { log.trace("ignoring exception on " + id); }
               
               break;
            }
         }
      }
            
      postDeliver(sess, consumerID, m, isConnectionConsumer);          
   }
   
   protected static void preDeliver(SessionDelegate sess,
                                    int consumerID,
                                    MessageProxy m,
                                    boolean isConnectionConsumer)
      throws JMSException
   {
      // If this is the callback-handler for a connection consumer we don't want to acknowledge or
      // add anything to the tx for this session.
      if (!isConnectionConsumer)
      {
         sess.preDeliver(m, consumerID);
      }         
   }
   
   protected static void postDeliver(SessionDelegate sess,
                                     int consumerID,
                                     MessageProxy m,
                                     boolean isConnectionConsumer)
      throws JMSException
   {
      // If this is the callback-handler for a connection consumer we don't want to acknowledge or
      // add anything to the tx for this session
      if (!isConnectionConsumer)
      {
         sess.postDeliver();
      }         
   }
   
   // Attributes ----------------------------------------------------
      
   private LinkedList buffer;
   
   private SessionDelegate sessionDelegate;
   
   private ConsumerDelegate consumerDelegate;
   
   private int consumerID;
   
   private boolean isConnectionConsumer;
   
   private volatile Thread receiverThread;
   
   private MessageListener listener;
    
   private int ackMode;
      
   private boolean closed;
   
   private Object mainLock;
   
   private boolean serverSending;
   
   private int bufferSize;
   
   private QueuedExecutor sessionExecutor;
   
   private boolean listenerRunning;
        
   // Constructors --------------------------------------------------

   public MessageCallbackHandler(boolean isCC, int ackMode,                                
                                 SessionDelegate sess, ConsumerDelegate cons, int consumerID,
                                 int bufferSize, QueuedExecutor sessionExecutor)
   {
      if (bufferSize < 1)
      {
         throw new IllegalArgumentException(this + " bufferSize must be > 0");
      }
              
      this.bufferSize = bufferSize;
      
      buffer = new LinkedList();
      
      isConnectionConsumer = isCC;
      
      this.ackMode = ackMode;
      
      this.sessionDelegate = sess;
      
      this.consumerDelegate = cons;
      
      this.consumerID = consumerID;
      
      this.serverSending = true;
      
      mainLock = new Object();                  
      
      this.sessionExecutor = sessionExecutor;
   }
        
   // Public --------------------------------------------------------
      

   /**
    * Handles a list of messages sent from the server
    * @param msgs The list of messages
    * @return The number of messages handled (placeholder for future - now we always accept all messages)
    *         or -1 if closed
    */
   public HandleMessageResponse handleMessage(List msgs) throws HandleCallbackException
   {            
      if (trace) { log.trace(this + " receiving " + msgs.size() + " messages from the remoting layer"); }            
                      
      synchronized (mainLock)
      {
         if (closed)
         {             
            //Ignore
            return new HandleMessageResponse(false, 0);
         }
                                      
         //Put the messages in the buffer
         //And notify any waiting receive()
         
         processMessages(msgs);
                   
         buffer.addAll(msgs);                  
         
         if (trace) { log.trace(this + " added messages to the buffer"); }            
         
         boolean full = buffer.size() >= bufferSize;         
         
         if (trace) { log.trace(this + " receiving messages from the remoting layer"); }            
                   
         messagesAdded();
         
         if (full)
         {
            serverSending = false;
         }
                                          
         //For now we always accept all messages - in the future this may change
         return new HandleMessageResponse(full, msgs.size());
      }
   }
         
   public void setMessageListener(MessageListener listener) throws JMSException
   {     
      synchronized (mainLock)
      {
         if (receiverThread != null)
         {
            // Should never happen
            throw new IllegalStateException("Consumer is currently in receive(..) Cannot set MessageListener");
         }
         
         this.listener = listener;
                            
         if (listener != null && !buffer.isEmpty())
         {  
            listenerRunning = true;
            this.queueRunner(new ListenerRunner());
         }        
      }   
   }
      
   public void close() throws JMSException
   {   
      synchronized (mainLock)
      {
         log.debug(this + " closing");
         
         if (closed)
         {
            return;
         }
         
         closed = true;   
         
         if (receiverThread != null)
         {            
            //Wake up any receive() thread that might be waiting
            mainLock.notify();
         }   
         
         this.listener = null;
      }
         
      waitForOnMessageToComplete();
      
      //Now we cancel anything left in the buffer
      //The reason we do this now is that otherwise the deliveries wouldn't get cancelled
      //until session close (since we don't cancel consumer's deliveries until then)
      //which is too late - since we need to preserve the order of messages delivered in a session.
      
      if (!buffer.isEmpty())
      {            
         //Now we cancel any deliveries that might be waiting in our buffer
         //This is because, otherwise the messages wouldn't get cancelled until
         //the corresponding session died.
         //So if another consumer in another session tried to consume from the channel
         //before that session died it wouldn't receive those messages
         Iterator iter = buffer.iterator();
         
         List ackInfos = new ArrayList();
         while (iter.hasNext())
         {                        
            MessageProxy mp = (MessageProxy)iter.next();
            
            AckInfo ack = new AckInfo(mp, consumerID);
            
            ackInfos.add(ack);
            
         }
               
         sessionDelegate.cancelDeliveries(ackInfos);
         
         buffer.clear();
      }                
      
      if (trace) { log.trace(this + " closed"); }
   }
   
   private void waitForOnMessageToComplete()
   {
      //Wait for any on message executions to complete
      
      Future result = new Future();
      
      try
      {
         this.sessionExecutor.execute(new Closer(result));
         
         result.getResult();
      }
      catch (InterruptedException e)
      {
         log.warn("Thread interrupted", e);
      }
   }
     
   /**
    * Method used by the client thread to get a Message, if available.
    *
    * @param timeout - the timeout value in milliseconds. A zero timeount never expires, and the
    *        call blocks indefinitely. A -1 timeout means receiveNoWait(): return the next message
    *        or null if one is not immediately available. Returns null if the consumer is
    *        concurrently closed.
    */
   public MessageProxy receive(long timeout) throws JMSException
   {                
      MessageProxy m = null;      
      
      synchronized (mainLock)
      {        
         if (trace) { log.trace(this + " receiving, timeout = " + timeout); }
         
         if (closed)
         {
            //If consumer is closed or closing calling receive returns null
            return null;
         }
         
         if (listener != null)
         {
            throw new JMSException("The consumer has a MessageListener set, cannot call receive(..)");         
         }
                       
         receiverThread = Thread.currentThread();
               
         long startTimestamp = System.currentTimeMillis();
                  
         try
         {
            while(true)
            {                             
               if (timeout == 0)
               {
                  if (trace) { log.trace("receive with no timeout"); }
                  
                  m = getMessage(0);                     
                  
                  if (m == null)
                  {
                     return null;
                  }
               }
               else if (timeout == -1)
               {
                  //ReceiveNoWait
                  if (trace) { log.trace("receive noWait"); }                  
                  
                  m = getMessage(-1);                     
                  
                  if (m == null)
                  {
                     if (trace) { log.trace("no message available"); }
                     return null;
                  }
               }
               else
               {
                  if (trace) { log.trace("receive timeout " + timeout + " ms, blocking poll on queue"); }
                  
                  m = getMessage(timeout);
                                    
                  if (m == null)
                  {
                     // timeout expired
                     if (trace) { log.trace(timeout + " ms timeout expired"); }
                     
                     return null;
                  }
               }
                              
               if (trace) { log.trace("received " + m + " after being blocked on the buffer"); }
                       
               //If message is expired we still call pre and post deliver
               //this makes sure the message is acknowledged so it gets removed
               //from the queue/subscription
               preDeliver(sessionDelegate, consumerID, m, isConnectionConsumer);
               
               postDeliver(sessionDelegate, consumerID, m, isConnectionConsumer);
               
               if (!m.getMessage().isExpired())
               {
                  if (trace) { log.trace("message " + m + " is not expired, pushing it to the caller"); }
                  
                  break;
               }
               
               if (trace)
               {
                  log.trace("message expired, discarding");
               }
               
               // the message expired, so discard the message, adjust timeout and reenter the buffer
               if (timeout != 0)
               {
                  timeout -= System.currentTimeMillis() - startTimestamp;
               }                            
            }           
         }
         finally
         {
            receiverThread = null;            
         }
      } 
      
      //This needs to be outside the lock
      if (buffer.isEmpty() && !serverSending)
      {
         //The server has previously stopped sending because the buffer was full
         //but now it is empty, so we tell the server to start sending again
         consumerDelegate.more();
      }
      
      return m;
   }    
   

   public MessageListener getMessageListener()
   {
      return listener;      
   }

   public String toString()
   {
      return "MessageCallbackHandler[" + consumerID + "]";
   }
   
   public int getConsumerId()
   {
      return consumerID;
   }
   
   public void addToFrontOfBuffer(MessageProxy proxy)
   {
      synchronized (mainLock)
      {
         buffer.addFirst(proxy);
         
         messagesAdded();
      }
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected long waitOnLock(Object lock, long waitTime) throws InterruptedException
   {
      long start = System.currentTimeMillis();
      
      //Wait for last message to arrive
      lock.wait(waitTime);
     
      long waited = System.currentTimeMillis() - start;
      
      if (waited < waitTime)
      {
         waitTime = waitTime - waited;
         
         return waitTime;
      }
      else
      {
         return 0;
      }     
   }
        
   protected MessageProxy getMessage(long timeout) throws JMSException
   {
      if (timeout == -1)
      {
         // receiveNoWait so don't wait
      }
      else
      {         
         try
         {         
            if (timeout == 0)
            {
               // Wait for ever potentially
               while (!closed && buffer.isEmpty())
               {
                  mainLock.wait();               
               }
            }
            else
            {
               // Wait with timeout
               long toWait = timeout;
             
               while (!closed && buffer.isEmpty() && toWait > 0)
               {
                  if (trace) { log.trace("Waiting on lock"); }
                  toWait = waitOnLock(mainLock, toWait);
                  if (trace) { log.trace("Done waiting on lock, empty?" + buffer.isEmpty()); }
               }
            }
         }
         catch (InterruptedException e)
         {
            return null;
         } 
      }
             
      if (closed)
      {
         return null;
      }
         
      MessageProxy m = null;     
      
      if (!buffer.isEmpty())
      {
         m = (MessageProxy)buffer.removeFirst();
         
         if (trace) { log.trace("Got message:" + m); }                  
      }
      else
      {
         m = null;
      }
     
      return m;
   }
   
   protected void processMessages(List msgs)
   {
      Iterator iter = msgs.iterator();
      
      while (iter.hasNext())
      {         
         MessageProxy msg = (MessageProxy)iter.next();
      
         //if this is the handler for a connection consumer we don't want to set the session delegate
         //since this is only used for client acknowledgement which is illegal for a session
         //used for an MDB
         msg.setSessionDelegate(sessionDelegate, isConnectionConsumer);
                  
         msg.setReceived();
      }
   }
   
   // Private -------------------------------------------------------
   
   private void queueRunner(ListenerRunner runner)
   {
      try
      {
         this.sessionExecutor.execute(runner);
      }
      catch (InterruptedException e)
      {
         log.warn("Thread interrupted", e);
      }
   }
   
   private void messagesAdded()
   {
      //If we have a thread waiting on receive() we notify it
      if (receiverThread != null)
      {
         if (trace) { log.trace(this + " notifying receiver thread"); }            
         mainLock.notify();
      }     
      else if (listener != null)
      { 
         //We have a message listener
         if (!listenerRunning)
         {
            listenerRunning = true;
            this.queueRunner(new ListenerRunner());
         }     
         
         //TODO - Execute onMessage on same thread for even better throughput 
      }
   }
   
   // Inner classes -------------------------------------------------   
   
   /*
    * This class is used to put on the listener executor to wait for onMessage
    * invocations to complete when closing
    */
   private class Closer implements Runnable
   {
      Future result;
      
      Closer(Future result)
      {
         this.result = result;
      }
      
      public void run()
      {
         result.setResult(null);
      }
   }
   
   /*
    * This class handles the execution of onMessage methods
    */
   private class ListenerRunner implements Runnable
   {
      public void run()
      {         
         MessageProxy mp = null;
         
         boolean again = false;
           
         synchronized (mainLock)
         {
            if (listener == null)
            {
               listenerRunning = false;
               
               return;
            }
            
            //remove a message from the buffer

            if (buffer.isEmpty())
            {
               listenerRunning = false;               
            }
            else
            {               
               mp = (MessageProxy)buffer.removeFirst();
               
               if (mp == null)
               {
                  throw new java.lang.IllegalStateException("Cannot find message in buffer!");
               }
               
               again = !buffer.isEmpty();
               
               if (!again)
               {
                  listenerRunning  = false;
               }  
            }
         }
                        
         if (mp != null)
         {
            try
            {
               callOnMessage(consumerDelegate, sessionDelegate, listener, consumerID, false, mp, ackMode);
            }
            catch (JMSException e)
            {
               log.error("Failed to deliver message", e);
            } 
         }
         
         if (again)
         {
            //Queue it up again
            queueRunner(this);
         }
         else
         {
            if (!serverSending)
            {
               //Ask server for more messages
               try
               {
                  consumerDelegate.more();
               }
               catch (JMSException e)
               {
                  log.error("Failed to execute more()", e);
               }
               return;
            }
         }
      }
   }
}




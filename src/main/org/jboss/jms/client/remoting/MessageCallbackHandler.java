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

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.MessageProxy;
import org.jboss.logging.Logger;
import org.jboss.remoting.callback.HandleCallbackException;

import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
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
                  
      try
      {      
         listener.onMessage(m);         
      }
      catch (RuntimeException e)
      {
         long id = m.getMessage().getMessageID();

         log.error("RuntimeException was thrown from onMessage, " + id + " will be redelivered", e);
         
         //See JMS1.1 spec 4.5.2

         if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE)
         {
            //Cancel the message - this means it will be immediately redelivered
            if (trace) { log.trace("cancelling " + id); }
            cons.cancelMessage(id);
         }
         else
         {
            //Session is either transacted or CLIENT_ACKNOWLEDGE
            //We just deliver next message
            if (trace) { log.trace("ignoring exception on " + id); }
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
      //If this is the callback-handler for a connection consumer we don't want
      //to acknowledge or add anything to the tx for this session
      if (!isConnectionConsumer)
      {
         sess.preDeliver(m.getMessage().getMessageID(), consumerID);
      }         
   }
   
   protected static void postDeliver(SessionDelegate sess,
                                     int consumerID,
                                     MessageProxy m,
                                     boolean isConnectionConsumer)
      throws JMSException
   {
      //If this is the callback-handler for a connection consumer we don't want
      //to acknowledge or add anything to the tx for this session
      if (!isConnectionConsumer)
      {
         sess.postDeliver(m.getMessage().getMessageID(), consumerID);
      }         
   }
   
   // Attributes ----------------------------------------------------
      
   protected LinkedList buffer;
   
   protected SessionDelegate sessionDelegate;
   
   protected ConsumerDelegate consumerDelegate;
   
   protected int consumerID;
   
   protected boolean isConnectionConsumer;
   
   protected volatile Thread receiverThread;
   
   protected MessageListener listener;
   
   protected int deliveryAttempts;
   
   protected int ackMode;
    
   // Executor used for executing onMessage methods - there is one per session
   protected QueuedExecutor onMessageExecutor;
   
   // Executor for executing activateConsumer methods asynchronously, there is one pool per connection
   protected PooledExecutor activateConsumerExecutor;
       
   protected Object mainLock;
   
   protected Object activationLock;
   
   protected Object onMessageLock;
   
   protected boolean closed;
      
   protected boolean closing;
   
   protected boolean gotLastMessage;
   
   //The id of the last message we received
   protected long lastMessageId = -1;
   
   protected volatile int activationCount;
   
   protected volatile boolean onMessageExecuting;
   
   // Constructors --------------------------------------------------

   public MessageCallbackHandler(boolean isCC, int ackMode, QueuedExecutor onMessageExecutor,
                                 PooledExecutor activateConsumerExecutor,
                                 SessionDelegate sess, ConsumerDelegate cons, int consumerID)
   {
      buffer = new LinkedList();
      
      isConnectionConsumer = isCC;
      
      this.ackMode = ackMode;
      
      this.onMessageExecutor = onMessageExecutor;
      
      this.activateConsumerExecutor = activateConsumerExecutor;
         
      this.sessionDelegate = sess;
      
      this.consumerDelegate = cons;
      
      this.consumerID = consumerID;
      
      mainLock = new Object();
        
      activationLock = new Object();      
      
      onMessageLock = new Object();
   }
        
   // Public --------------------------------------------------------
   
   public void handleMessage(MessageProxy md) throws HandleCallbackException
   {            
      if (trace) { log.trace("receiving message " + md + " from the remoting layer"); }
      
      md = processMessage(md);
      
      synchronized (mainLock)
      {
         if (closed)
         {
            //Sanity check
            //This should never happen
            //Part of the close procedure is to ensure that no more messages will be sent
            //If this happens it implies the close() procedure is not functioning correctly
            throw new IllegalStateException("Message has arrived after consumer is closed!");
         }
         
         if (closing && gotLastMessage)
         {
            //Sanity check - this should never happen
            //No messages should arrive after the last one sent by the server consumer endpoint
            throw new IllegalStateException("Message has arrived after we have received the last one");
         }
         
         //We record the last message we received
         this.lastMessageId = md.getMessage().getMessageID();
                                 
         if (listener != null)
         {
            //Queue the message to be delivered by the session
            ClientDeliveryRunnable cdr = new ClientDeliveryRunnable(md);
            
            onMessageExecuting = true;         
            
            try
            {
               onMessageExecutor.execute(cdr);
            }
            catch (InterruptedException e)
            {
               //This should never happen
               throw new IllegalStateException("Thread interrupted in client delivery executor");
            }
         }
         else
         {                                                    
            //Put the message in the buffer
            //And notify any waiting receive()
            //On close any remaining messages will be cancelled
            //We do not wait for the message to be received before returning
                  
            buffer.add(md);                                 
         }   
         
         if (closing)
         {
            //If closing then we may have the close() thread waiting for the last message as well as a receive
            //thread
            mainLock.notifyAll();
         }
         else
         {
            //Otherwise we will only have at most one receive thread waiting
            //We don't want to do notifyAll in both cases since notifyAll can have a perf penalty
            if (receiverThread != null)
            {
               mainLock.notify();
            }
         }
      }
   }
    
   public void setMessageListener(MessageListener listener) throws JMSException
   {
      synchronized (mainLock)
      {         
         if (closed)
         {
            throw new JMSException("Cannot set MessageListener - consumer is closed");
         }
         
         // JMS consumer is single threaded, so it shouldn't be possible to set a MessageListener
         // while another thread is receiving
         
         if (receiverThread != null)
         {
            // Should never happen
            throw new javax.jms.IllegalStateException("Consumer is currently in receive(..) Cannot set MessageListener");
         }
         
         synchronized (onMessageLock)
         {         
            this.listener = listener;
         }
   
         if (trace) { log.trace("installed listener " + listener); }
   
         activateConsumer();
      }
   }
          
   public void close() throws JMSException
   {
      try
      {
         synchronized (mainLock)
         {
            if (closed)
            {
               return;
            }
            
            closing = true;   
            
            //We wait for any activation in progress to complete and the resulting message
            //(if any) to be returned and processed.
            //The ensures a clean, gracefully closure of the client side consumer, without
            //any messages in transit which might arrive after the consumer is closed and which
            //subsequently might be cancelled out of sequence causing message ordering problems
            waitForActivationsToComplete();
               
            //Now we know there are no activations in progress but the consumer may still be active so we call
            //deactivate which returns the id of the last message we should have received
            //if we have received this message then we know there is no possibility of any message still in
            //transit and we can close down with confidence
            //otherwise we wait for this message and timeout if it doesn't arrive which might be the case
            //if the connection to the server has been lost
            
            //TODO Make configurable
            final int TIMEOUT = 20000;
            
            long lastMessageIDToExpect = deactivateConsumer();
            
            if (lastMessageIDToExpect != -1)
            {            
               long waitTime = TIMEOUT;
               
               while (lastMessageIDToExpect != lastMessageId && waitTime > 0)
               {               
                  waitTime = waitOnLock(mainLock, waitTime);           
               }
               
               if (lastMessageIDToExpect != lastMessageId)
               {
                  log.warn("Timed out waiting for last message to arrive, last=" + lastMessageId +" expected=" + lastMessageIDToExpect);
               }
            }
            
            //We set this even if we timed out waiting since we do not want any more to arrive now
            gotLastMessage = true;            
            
            //Wake up any receive() thread that might be waiting
            mainLock.notify();
            
            //Now make sure that any onMessage of a listener has finished executing
            
            long waitTime = TIMEOUT;
            
            synchronized (onMessageLock)
            {               
               while (onMessageExecuting && waitTime > 0)
               {
                  waitTime = waitOnLock(onMessageLock, waitTime);   
               }
               if (onMessageExecuting)
               {
                  //Timed out waiting for last onMessage to be processed
                  log.warn("Timed out waiting for last onMessage to be executed");            
               }
            }
                        
            //Now we know that all messages have been received and processed                                 
            
            if (!buffer.isEmpty())
            {            
               //Now we cancel any deliveries that might be waiting in our buffer
               Iterator iter = buffer.iterator();
               
               List ids = new ArrayList();
               while (iter.hasNext())
               {                        
                  MessageProxy mp = (MessageProxy)iter.next();
                  
                  ids.add(new Long(mp.getMessage().getMessageID()));                                    
               }
               cancelMessages(ids);
            }
          
            //Now we are done
            listener = null;
            
            receiverThread = null;
            
            closed = true;  
         }
      }
      catch (InterruptedException e)
      {
         //No one should be interrupting the thread so this shouldn't occur
         throw new IllegalStateException("Thread interrupted while closing consumer");
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
      synchronized (mainLock)
      {         
         if (closed)
         {
            throw new JMSException("Cannot call receive(..) Consumer is closed");
         }
         
         if (listener != null)
         {
            throw new JMSException("The consumer has a MessageListener set, cannot call receive(..)");         
         }
                       
         receiverThread = Thread.currentThread();
               
         long startTimestamp = System.currentTimeMillis();
         
         MessageProxy m = null;
         
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
                                  
               if (!m.getMessage().isExpired())
               {
                  if (trace) { log.trace("message " + m + " is not expired, pushing it to the caller"); }
                  
                  preDeliver(sessionDelegate, consumerID, m, isConnectionConsumer);
                  
                  postDeliver(sessionDelegate, consumerID, m, isConnectionConsumer);
                  
                  return m;
               }
               
               log.debug("message expired, discarding");
               
               // the message expired, so discard the message, adjust timeout and reenter the buffer
               if (timeout != 0)
               {
                  timeout -= System.currentTimeMillis() - startTimestamp;
               }
               
               if (closing)
               {
                  return null;
               }               
            }
         }
         finally
         {
            receiverThread = null;            
         }
      } 
   }    
   
   public MessageListener getMessageListener()
   {
      synchronized (onMessageLock)
      {
         return listener;
      }
   }

   public String toString()
   {
      return "MessageCallbackHandler[" + consumerID + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void waitForActivationsToComplete()
   {
      synchronized (activationLock)
      {
         while (activationCount > 0)
         {
            try
            {
               activationLock.wait();              
            }
            catch (InterruptedException e)
            {
               //This should never occur
               throw new IllegalStateException("Thread interrupted in waiting for activation count to reach zero");
            }
         }
      }
   }
    
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
    
   protected void cancelMessages(List ids)
   {
      try
      {
         consumerDelegate.cancelMessages(ids);
      }
      catch (Exception e)
      {
         String msg = "Failed to cancel messages";
         log.warn(msg, e);         
      }
   }
   
   protected void activateConsumer() throws JMSException
   {
      // We execute this on a separate thread to avoid the case where the asynchronous delivery
      // arrives before we have returned from the synchronus call, which would cause us to lose
      // the message
        
      try
      {
         if (trace) { log.trace("initiating consumer endpoint activation"); }
         activationCount++;         
         activateConsumerExecutor.execute(new ConsumerActivationRunnable());
      }
      catch (InterruptedException e)
      {
         //This should never happen
         throw new IllegalStateException("Activation executor thread interrupted");
      }
   }
   
   protected long deactivateConsumer() throws JMSException
   {
      return consumerDelegate.deactivate();
   }
   
   protected MessageProxy getMessageNow() throws JMSException
   {
      MessageProxy del = (MessageProxy)consumerDelegate.getMessageNow(false);      
      
      if (del != null)
      {
         //We record the id of the last message delivered
         //No need to notify here since this will never be called while we
         //are closing
         lastMessageId = del.getMessage().getMessageID();         
                  
         return processMessage(del);
      }
      else
      {
         return null;
      }
   }
   
   protected MessageProxy getMessage(long timeout) throws JMSException
   {
      MessageProxy m = null;
      
      // If it's receiveNoWait then get the message directly
      if (timeout == -1)
      {
         m = getMessageNow();        
      }
      else
      {
         // ... otherwise we activate the server side consumer and wait for a message to arrive
         // asynchronously         
         activateConsumer();
      
         try
         {         
            if (timeout == 0)
            {
               //Wait for ever potentially
               while (!closing && buffer.isEmpty())
               {
                  mainLock.wait();               
               }
            }
            else
            {
               //Wait with timeout
               long toWait = timeout;
             
               while (!closing && buffer.isEmpty() && toWait > 0)
               {
                  toWait = waitOnLock(mainLock, toWait);
               }
            }
             
            if (closing)
            {
               m = null;
            }
            else
            {
               if (!buffer.isEmpty())
               {
                  m = (MessageProxy)buffer.removeFirst();
               }
               else
               {
                  m = null;
               }
            }
         }
         catch (InterruptedException e)
         {
            //No one should be interrupting the thread so this shouldn't occur
            throw new IllegalStateException("Thread interrupted while receiving");
         }         
         finally
         {
            // We only need to call this if we timed out        
            if (m == null)
            {
               deactivateConsumer();
            }               
         } 
      }
               
      return m;
   }
   
   protected MessageProxy processMessage(MessageProxy del)
   {
      //if this is the handler for a connection consumer we don't want to set the session delegate
      //since this is only used for client acknowledgement which is illegal for a session
      //used for an MDB
      if (!this.isConnectionConsumer)
      {
         del.setSessionDelegate(sessionDelegate);
      }         
      del.setReceived();
      
      return del;
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   private class ClientDeliveryRunnable implements Runnable
   {
      private MessageProxy message;
      
      private ClientDeliveryRunnable(MessageProxy message)
      {
         this.message = message;
      }
      
      public void run()
      {
         //We synchronize here to prevent the message listener being set with a different one
         //between callOnMessage and activate being called
         synchronized (onMessageLock)
         { 
            if (closed)
            {
               //Sanity check. This should never happen     
               //Part of the close procedure is to ensure there are no messages in the executor queue
               //for delivery to the MessageListener
               //If this happens it implies the close() procedure is not working properly
               throw new IllegalStateException("Calling onMessage() but the consumer is closed!");
            }
            else
            {
               try
               {                                                    
                  MessageCallbackHandler.callOnMessage(consumerDelegate, sessionDelegate, listener,
                                                       consumerID, isConnectionConsumer, message, ackMode);
                  if (!closing)
                  {
                     consumerDelegate.activate();                                  
                  }
                  
                  onMessageExecuting = false;
                  
                  //The close() thread may be waiting for us to finish executing, so wake it up
                  onMessageLock.notify();                 
               }
               catch (JMSException e)
               {
                  log.error("Failed to deliver message", e);
               }                           
            }
         }
      }
   }
   
   private class ConsumerActivationRunnable implements Runnable
   {
      public void run()
      {      
         try
         {
            //We always try and return the message immediately, if available.
            //This prevents an extra network call to deliver the message.
            //If the message is not available, the consumer will stay active and
            //the message will delivered asynchronously (pushed) (that is what the boolean param is for)
            
            MessageProxy m = (MessageProxy)consumerDelegate.getMessageNow(true);
               
            if (m != null)
            {
               handleMessage(m);
            }
                
            synchronized (activationLock)
            {
               activationCount--;
               if (activationCount == 0)
               {
                  activationLock.notify();
               }
            }            
         }
         catch(Throwable t)
         {
            log.error("Consumer endpoint activation failed", t);
            if (t.getCause() != null)
            {
               log.error("Cause:" + t.getCause());
            }
            try
            {
               close();
            }
            catch (JMSException e)
            {
               log.error("Failed to close consumer", e);
            }
         }        
      } 
   }      
}



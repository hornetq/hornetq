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

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.MessageDelegate;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.Util;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.HandleCallbackException;
import org.jboss.remoting.callback.InvokerCallbackHandler;

import EDU.oswego.cs.dl.util.concurrent.Executor;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;
import EDU.oswego.cs.dl.util.concurrent.SynchronousChannel;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox/a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageCallbackHandler implements InvokerCallbackHandler
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(MessageCallbackHandler.class);
   
   // Static --------------------------------------------------------
   
   public static void callOnMessage(ConsumerDelegate cons,
                                    SessionDelegate sess,
                                    MessageListener listener,
                                    String consumerID,
                                    boolean isConnectionConsumer,
                                    Message m,
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
         Serializable id = m.getJMSMessageID();

         log.error("RuntimeException was thrown from onMessage, " + Util.guidToString(id) + " will be redelivered", e);
         
         //See JMS1.1 spec 4.5.2

         if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE)
         {
            //Cancel the message - this means it will be immediately redelivered

            if (log.isTraceEnabled()) { log.trace("cancelling " + Util.guidToString(id)); }
            cons.cancelMessage(id);
         }
         else
         {
            //Session is either transacted or CLIENT_ACKNOWLEDGE
            //We just deliver next message
            if (log.isTraceEnabled()) { log.trace("ignoring exception on " + Util.guidToString(id)); }
         }
      }
            
      postDeliver(sess, consumerID, m, isConnectionConsumer);
          
   }
   
   protected static void preDeliver(SessionDelegate sess,
                                    String consumerID,
                                    Message m,
                                    boolean isConnectionConsumer)
      throws JMSException
   {
      //If this is the callback-handler for a connection consumer we don't want
      //to acknowledge or add anything to the tx for this session
      if (!isConnectionConsumer)
      {
         sess.preDeliver(m.getJMSMessageID(), consumerID);
      }         
   }
   
   protected static void postDeliver(SessionDelegate sess,
                                     String consumerID,
                                     Message m,
                                     boolean isConnectionConsumer)
      throws JMSException
   {
      //If this is the callback-handler for a connection consumer we don't want
      //to acknowledge or add anything to the tx for this session
      if (!isConnectionConsumer)
      {
         sess.postDeliver(m.getJMSMessageID(), consumerID);
      }         
   }
   
   // Attributes ----------------------------------------------------

   protected SynchronousChannel buffer;
   
   protected SessionDelegate sessionDelegate;
   
   protected ConsumerDelegate consumerDelegate;
   
   protected String consumerID;
   
   protected boolean isConnectionConsumer;
   
   protected boolean closed;
   
   protected volatile Thread receiverThread;
   
   protected Object receiverLock;
   
   protected volatile MessageListener listener;
   
   protected volatile boolean stopping;
   
   protected volatile boolean waiting;
   
   protected int deliveryAttempts;
   
   protected int ackMode;
   
   //Executor used for executing onMessage methods - there is one per session
   protected Executor onMessageExecutor;
   
   //Executor for executing activateConsumer methods asynchronously - there is one pool per connection
   protected Executor pooledExecutor;
   
   //We need to keep track of how many calls to activate we have made so when we close the 
   //consumer we can wait for the last one to complete otherwise
   //we can end up with closing the consumer and then a call to activate occurs
   //causing an exception
   protected SynchronizedInt activationCount;
   

   // Constructors --------------------------------------------------

   public MessageCallbackHandler(boolean isCC, int ackMode, Executor executor, Executor pooledExecutor)
   {
      buffer = new SynchronousChannel();
      
      isConnectionConsumer = isCC;
       
      receiverLock = new Object();
      
      this.ackMode = ackMode;
      
      this.onMessageExecutor = executor;
      
      this.pooledExecutor = pooledExecutor;
      
      activationCount = new SynchronizedInt(0);
   }

   // InvokerCallbackHandler implementation -------------------------
   
   public synchronized void handleCallback(Callback callback) throws HandleCallbackException
   {
      if (log.isTraceEnabled()) { log.trace("receiving message " + callback.getParameter() + " from the remoting layer"); }

      if (closed)
      {
         log.warn("Consumer is closed - ignoring message");
         //Note - we do not cancel the message if the handler is closed.
         //If the handler is closed then the corresponding serverconsumerdelegate
         //is either already closed or about to close, in which case it's deliveries
         //will be cancelled anyway.
         return;
      }
      
      MessageDelegate md = (MessageDelegate)callback.getParameter();
      
      if (listener != null)
      {
         //Queue the message to be delivered by the session
         ClientDeliveryRunnable cdr = new ClientDeliveryRunnable(this, processMessage(md));
         try
         {
            onMessageExecutor.execute(cdr);
         }
         catch (InterruptedException e)
         {
            log.error("Thread was interrupted, cancelling message", e);
            cancelMessage(md);
         }
      }
      else
      {                                 
         try
         {                    
            //We attempt to put the message in the Channel
            //The call will return false if the message is not picked up the receiving or listening
            //thread in which case we need to cancel it
            
            boolean handled = false;
            while (waiting)
            {
               //channel.offer will *only* return true if there is a thread waiting to take a
               //message using take() or poll() hence we can guarantee there is no chance any
               //messages can arrive and are left in the channel without being handled -  this is
               //why we use a SynchronousChannel :)
               
               //We do this in a while loop to deal with a possible race condition where
               //waiting had been set but the main consumer thread hadn't quite blocked on the call
               //to take or poll from the channel
               
               handled = buffer.offer(processMessage(md), 0);
                              
               if (handled)
               {
                  break;
               } 
               
               //ust yield to other threads - otherwise all CPU is eaten in this loop and everything
               //slows down
               Thread.yield();               
            }
            
            if (!handled)
            {
               //There is no-one waiting for our message so we cancel it
               if (!closed)
               {
                  cancelMessage(md);
               }
            }              
         }
         catch(InterruptedException e)
         {
            String msg = "Interrupted attempt to put message in the delivery buffer";
            log.error(msg);
            throw new HandleCallbackException(msg, e);
         }
      }
      
   }
   
  
   // Public --------------------------------------------------------
    
   public synchronized void setMessageListener(MessageListener listener) throws JMSException
   {
      //JMS consumer is single threaded, so it shouldn't be possible to
      //set a MessageListener while another thread is receiving
      
      if (receiverThread != null)
      {
         //Should never happen
         throw new javax.jms.IllegalStateException("Another thread is already receiving");
      }
      
      this.listener = listener;            
      
      activateConsumer();      
   }
 
   
   public synchronized void close()
   {
      closed = true;
          
      stopReceiver();       
      
      //There may still be pending calls to activateConsumer - we wait for them to complete (or fail)
      while (activationCount.get() != 0)
      {
         Thread.yield();
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
   public Message receive(long timeout)
      throws JMSException
   {                 
      
      if (listener != null)
      {
         throw new JBossJMSException("A message listener is already registered");         
      }
                    
      receiverThread = Thread.currentThread();
            
      long startTimestamp = System.currentTimeMillis();
      
      Message m = null;
      
      try
      {
         while(true)
         {
            try
            {
               if (timeout == 0)
               {
                  if (log.isTraceEnabled()) log.trace("receive with no timeout");
                  
                  if (!stopping)
                  {
                     m = getMessage(0);
                  }
                  
                  if (m == null)
                  {
                     return null;
                  }
                  
                  if (log.isTraceEnabled()) { log.trace("got " + m); }
               }
               else if (timeout == -1)
               {
                  //ReceiveNoWait
                  if (log.isTraceEnabled()) { log.trace("receive noWait"); }                  
                  
                  if (!stopping)
                  {
                     m = getMessage(-1);
                  }
                  
                  if (m == null)
                  {
                     if (log.isTraceEnabled()) { log.trace("no message available"); }
                     return null;
                  }
               }
               else
               {
                  if (log.isTraceEnabled()) { log.trace("receive timeout " + timeout + " ms, blocking poll on queue"); }
                  
                  if (!stopping)
                  {
                     m = getMessage(timeout);
                  }
                  
                  if (m == null)
                  {
                     // timeout expired
                     if (log.isTraceEnabled()) { log.trace(timeout + " ms timeout expired"); }
                     
                     return null;
                  }
               }
            }
            catch(InterruptedException e)
            {
               if (log.isTraceEnabled()) { log.trace("Thread was interrupted"); }
               
               if (closed)
               {
                  return null;
               }
               else
               {
                  throw new JMSException("Interrupted");
               }
            }
            
            if (log.isTraceEnabled()) { log.trace("got " + m); }
                               
            if (!((MessageDelegate)m).getMessage().isExpired())
            {
               if (log.isTraceEnabled()) { log.trace("message " + m + " is not expired, returning it to the caller"); }
               
               preDeliver(sessionDelegate, consumerID, m, isConnectionConsumer);
               
               postDeliver(sessionDelegate, consumerID, m, isConnectionConsumer);
               
               return m;
            }
            
            log.debug("message expired, discarding");
            
            // discard the message, adjust timeout and reenter the buffer
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
   
   public void setSessionDelegate(SessionDelegate delegate)
   {
      this.sessionDelegate = delegate;
   }
   
   public void setConsumerDelegate(ConsumerDelegate delegate)
   {
      this.consumerDelegate = delegate;
   }
   
   public void setConsumerID(String receiverID)
   {
      this.consumerID = receiverID;
   }
   
   public synchronized MessageListener getMessageListener()
   {
      return listener;
   }

   public String toString()
   {
      return "MessageCallbackHandler[" + consumerID + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void cancelMessage(Message m)
   {
      try
      {
         consumerDelegate.cancelMessage(m.getJMSMessageID());
      }
      catch (Exception e)
      {
         //It may be ok that we cannot cancel the message - e.g.
         //if the corresponding server consumer delegate is already closed
         //in which case the deliveries will be cancelled anyway
         String msg = "Failed to cancel message";
         log.warn(msg, e);         
      }
   }        
   
   protected void stopReceiver()
   {
      synchronized (receiverLock)
      {         
         stopping = true;
         
         //The listener loop may not be waiting, so interrupting the thread will
         //not necessarily work, thus leaving it hanging
         //so we use the listenerStopping variable too
         if (receiverThread != null)
         {
            receiverThread.interrupt();
         }
         
         //FIXME - There is a possibility the receiver thread could still be waiting inside the receive() method
         //after the interrupt - need to deal with this
         
         stopping = false;
      }
         
   }
   
   protected void activateConsumer() throws JMSException
   {
      //We execute this on a separate thread to avoid the case where the asynch delivery
      //arrives before we have returned from the synch call, which would
      //cause us to lose the message
      
      try
      {         
         pooledExecutor.execute(new ConsumerActivationRunnable());     
         activationCount.increment();
      }
      catch (InterruptedException e)
      {
         throw new JBossJMSException("Thread interrupted", e);
      }
   }
   
   protected void deactivateConsumer() throws JMSException
   {
      consumerDelegate.deactivate();
   }
   
   protected Message getMessageNow() throws JMSException
   {
      MessageDelegate del = (MessageDelegate)consumerDelegate.getMessageNow();
      if (del != null)
      {
         return processMessage(del);
      }
      else
      {
         return null;
      }
   }
   
   protected Message getMessage(long timeout) throws InterruptedException, JMSException
   {
      Message m = null;
      
      //If it's receiveNoWait then get the message directly
      if (timeout == -1)
      {
         waiting = false;
         
         m = getMessageNow();
          
      }
      else
      {
         //otherwise we active the server side consumer and 
         //wait for a message to arrive asynchronously
         
         waiting = true;
         
         activateConsumer();
      
         try
         {
            if (timeout == 0)
            {
               //Indefinite wait
               m = (Message)buffer.take();
            }            
            else
            {
               //wait with timeout
               m = (Message)buffer.poll(timeout);
            }
         }
         finally
         {
            //We only need to call this if we didn't receive a message synchronously
            waiting = false;
            
            if (!closed)
            {
               if (m == null)
               {
                  deactivateConsumer();
               }
            }
         }
      }
               
      return m;
   }
   
   protected Message processMessage(MessageDelegate del)
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
      private MessageCallbackHandler handler;
      
      private Message message;
      
      private ClientDeliveryRunnable(MessageCallbackHandler handler, Message message)
      {
         this.handler = handler;
         this.message = message;
      }
      
      public void run()
      {
         synchronized (handler)
         {
            if (handler.closed)
            {
               //The handler is closed - ignore the message - the serverconsumerdelegate will already be
               //closed so the message will have already been cancelled               
            }
            else
            {
               try
               {
                  MessageCallbackHandler.callOnMessage(consumerDelegate, sessionDelegate, listener,
                                                       consumerID, isConnectionConsumer, message, ackMode);
                  consumerDelegate.activate();
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
            if (log.isTraceEnabled()) { log.trace("activating consumer endpoint"); }
            consumerDelegate.activate();
         }
         catch(Throwable t)
         {
            log.error("Consumer endpoint activation failed", t);
            if (t.getCause() != null)
            {
               log.error("Cause:" + t.getCause());
            }
            stopReceiver();
         }
         finally
         {
            activationCount.decrement();
         } 
        
      } 
   }      
}


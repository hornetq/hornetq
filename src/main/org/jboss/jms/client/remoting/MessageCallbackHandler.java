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
import org.jboss.jms.server.endpoint.DefaultCancel;
import org.jboss.jms.server.endpoint.DeliveryInfo;
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
     
   //This is static so it can be called by the asf layer too
   public static void callOnMessage(SessionDelegate sess,
                                    MessageListener listener,
                                    int consumerID,
                                    long channelID,
                                    boolean isConnectionConsumer,
                                    MessageProxy m,
                                    int ackMode,
                                    int maxDeliveries,
                                    SessionDelegate connectionConsumerSession)
      throws JMSException
   {
      // If this is the callback-handler for a connection consumer we don't want to acknowledge or
      // add anything to the tx for this session.
      if (!isConnectionConsumer)
      {
         sess.preDeliver(new DeliveryInfo(m, consumerID, channelID, connectionConsumerSession));
      }  
                  
      int tries = 0;
      
      boolean cancel = false;
      
      while (true)
      {
         try
         {
            if (trace) { log.trace("calling listener's onMessage(" + m + ")"); }
            
            m.incDeliveryCount();
            
            listener.onMessage(m);

            if (trace) { log.trace("listener's onMessage() finished"); }
            
            break;
         }
         catch (RuntimeException e)
         {
            long id = m.getMessage().getMessageID();
   
            log.error("RuntimeException was thrown from onMessage, " + id + " will be redelivered", e);
            
            // See JMS 1.1 spec 4.5.2
   
            if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE)
            {
               // We redeliver a certain number of times
               if (tries < maxDeliveries)
               {                            
                  tries++;
               }
               else
               {
                  log.error("Max redeliveries has occurred for message: " + m.getJMSMessageID());
                  
                  // postdeliver will do a cancel rather than an ack which will cause the mesage
                  // to end up in the DLQ
                  
                  cancel = true;
                  
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

      if (!sess.isClosed())
      {
         // postDeliver only if the session is not closed

         // If this is the callback-handler for a connection consumer we don't want to acknowledge or
         // add anything to the tx for this session
         if (!isConnectionConsumer)
         {
            sess.postDeliver(cancel);
         }   
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
   private int maxDeliveries;
   private long channelID;
        
   // Constructors --------------------------------------------------

   public MessageCallbackHandler(boolean isCC, int ackMode,                                
                                 SessionDelegate sess, ConsumerDelegate cons, int consumerID,
                                 long channelID,
                                 int bufferSize, QueuedExecutor sessionExecutor,
                                 int maxDeliveries)
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
      this.channelID = channelID;
      this.serverSending = true;
      mainLock = new Object();
      this.sessionExecutor = sessionExecutor;
      this.maxDeliveries = maxDeliveries;
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
      if (trace)
      {
         StringBuffer sb = new StringBuffer(this + " receiving [");
         for(int i = 0; i < msgs.size(); i++)
         {
            sb.append(((MessageProxy)msgs.get(i)).getMessage().getMessageID());
            if (i < msgs.size() - 1)
            {
               sb.append(",");
            }
         }
         sb.append("] from the remoting layer");
         log.trace(sb.toString());
      }

      synchronized (mainLock)
      {
         if (closed)
         {             
            // Ignore
            return new HandleMessageResponse(false, 0);
         }

         // Asynchronously confirm delivery on client

         try
         {
            sessionExecutor.execute(new ConfirmDelivery(msgs.size()));
         }
         catch (InterruptedException e)
         {
            log.warn("Thread interrupted", e);
         }

         // Put the messages in the buffer and notify any waiting receive()
         
         processMessages(msgs);
                   
         buffer.addAll(msgs);                  
         
         if (trace) { log.trace(this + " added message(s) to the buffer"); }
         
         boolean full = buffer.size() >= bufferSize;         
         
         messagesAdded();
         
         if (full)
         {
            serverSending = false;
            if (trace) { log.trace(this + " is full"); }
         }
                                          
         // For now we always accept all messages - in the future this may change
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
            // Wake up any receive() thread that might be waiting
            mainLock.notify();
         }   
         
         this.listener = null;
      }

      waitForOnMessageToComplete();
      
      // Now we cancel anything left in the buffer. The reason we do this now is that otherwise the
      // deliveries wouldn't get cancelled until session close (since we don't cancel consumer's
      // deliveries until then), which is too late - since we need to preserve the order of messages
      // delivered in a session.
      
      if (!buffer.isEmpty())
      {            
         // Now we cancel any deliveries that might be waiting in our buffer. This is because
         // otherwise the messages wouldn't get cancelled until the corresponding session died.
         // So if another consumer in another session tried to consume from the channel before that
         // session died it wouldn't receive those messages.
         // We can't just cancel all the messages in the SCE since some of those messages might
         // have actually been delivered (unlike these) and we may want to acknowledge them
         // later, after this consumer has been closed

         List cancels = new ArrayList();

         for(Iterator i = buffer.iterator(); i.hasNext();)
         {
            MessageProxy mp = (MessageProxy)i.next();
            DefaultCancel ack = new DefaultCancel(mp.getDeliveryId(), mp.getDeliveryCount());
            cancels.add(ack);
         }
               
         sessionDelegate.cancelDeliveries(cancels);
         
         buffer.clear();
      }                
      
      if (trace) { log.trace(this + " closed"); }
   }
   
   private void waitForOnMessageToComplete()
   {
      // Wait for any onMessage() executions to complete

      if (Thread.currentThread().equals(sessionExecutor.getThread()))
      {
         // the current thread already closing this MessageCallbackHandler (this happens when the
         // session is closed from within the MessageListener.onMessage(), for example), so no need
         // to register another Closer (see http://jira.jboss.org/jira/browse/JBMESSAGING-542)
         return;
      }

      Future result = new Future();
      
      try
      {
         sessionExecutor.execute(new Closer(result));

         if (trace) { log.trace(this + " blocking wait for Closer execution"); }
         result.getResult();
         if (trace) { log.trace(this + " got Closer result"); }
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
            // If consumer is closed or closing calling receive returns null
            if (trace) { log.trace(this + " closed, returning null"); }
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
                  if (trace) { log.trace(this + ": receive, no timeout"); }
                  
                  m = getMessage(0);                     
                  
                  if (m == null)
                  {
                     return null;
                  }
               }
               else if (timeout == -1)
               {
                  //ReceiveNoWait
                  if (trace) { log.trace(this + ": receive, noWait"); }
                  
                  m = getMessage(-1);                     
                  
                  if (m == null)
                  {
                     if (trace) { log.trace(this + ": no message available"); }
                     return null;
                  }
               }
               else
               {
                  if (trace) { log.trace(this + ": receive, timeout " + timeout + " ms, blocking poll on queue"); }
                  
                  m = getMessage(timeout);
                                    
                  if (m == null)
                  {
                     // timeout expired
                     if (trace) { log.trace(this + ": " + timeout + " ms timeout expired"); }
                     
                     return null;
                  }
               }
                              
               if (trace) { log.trace(this + " received " + m + " after being blocked on buffer"); }
                       
               // If message is expired we still call pre and post deliver. This makes sure the
               // message is acknowledged so it gets removed from the queue/subscription.

               if (!isConnectionConsumer)
               {
                  sessionDelegate.preDeliver(new DeliveryInfo(m, consumerID, channelID, null));
                  
                  sessionDelegate.postDeliver(false);
               }
               
               //postDeliver(sessionDelegate, isConnectionConsumer, false);
               
               if (!m.getMessage().isExpired())
               {
                  if (trace) { log.trace(this + ": message " + m + " is not expired, pushing it to the caller"); }
                  
                  break;
               }
               
               if (trace)
               {
                  log.trace(this + ": message expired, discarding");
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
      
      m.incDeliveryCount();
      
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

   public void setConsumerId(int consumerId)
   {
       this.consumerID=consumerId;
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
      
      // Wait for last message to arrive
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
               // wait for ever potentially
               while (!closed && buffer.isEmpty())
               {
                  if (trace) { log.trace(this + " waiting on main lock, no timeout"); }

                  mainLock.wait();

                  if (trace) { log.trace(this + " done waiting on main lock"); }
               }
            }
            else
            {
               // wait with timeout
               long toWait = timeout;
             
               while (!closed && buffer.isEmpty() && toWait > 0)
               {
                  if (trace) { log.trace(this + " waiting on main lock, timeout " + toWait + " ms"); }

                  toWait = waitOnLock(mainLock, toWait);

                  if (trace) { log.trace(this + " done waiting on lock, buffer is " + (buffer.isEmpty() ? "" : "NOT ") + "empty"); }
               }
            }
         }
         catch (InterruptedException e)
         {
            if (trace) { log.trace("InterruptedException, " + this + ".getMessage() returning null"); }
            return null;
         } 
      }

      MessageProxy m = null;
             
      if (!closed && !buffer.isEmpty())
      {
         m = (MessageProxy)buffer.removeFirst();
      }

      if (trace) { log.trace("InterruptedException, " + this + ".getMessage() returning " + m); }
      return m;
   }
   
   protected void processMessages(List msgs)
   {
      Iterator iter = msgs.iterator();
      
      while (iter.hasNext())
      {         
         MessageProxy msg = (MessageProxy)iter.next();
      
         // If this is the handler for a connection consumer we don't want to set the session
         // delegate since this is only used for client acknowledgement which is illegal for a
         // session used for an MDB
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
      // If we have a thread waiting on receive() we notify it
      if (receiverThread != null)
      {
         if (trace) { log.trace(this + " notifying receiver thread"); }            
         mainLock.notify();
      }     
      else if (listener != null)
      { 
         // We have a message listener
         if (!listenerRunning)
         {
            listenerRunning = true;

            if (trace) { log.trace(this + " scheduled a new ListenerRunner"); }
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
         if (trace) { log.trace("Closer starts running"); }

         result.setResult(null);

         if (trace) { log.trace("Closer finished run"); }
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
               if (trace) { log.trace("no listener, returning"); }
               return;
            }
            
            // remove a message from the buffer

            if (buffer.isEmpty())
            {
               listenerRunning = false;
               if (trace) { log.trace("no messages in buffer, marking listener as not running"); }
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
                  if (trace) { log.trace("no more messages in buffer, marking listener as not running"); }
               }  
            }
         }
                        
         if (mp != null)
         {
            try
            {
               callOnMessage(sessionDelegate, listener, consumerID, channelID, false, mp, ackMode, maxDeliveries, null);
            }
            catch (JMSException e)
            {
               log.error("Failed to deliver message", e);
            } 
         }
         
         if (again)
         {
            // Queue it up again
            queueRunner(this);
         }
         else
         {
            if (!serverSending)
            {
               // Ask server for more messages
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

   /*
    * Used to asynchronously confirm to the server message arrival (delivery) on client.
    */
   private class ConfirmDelivery implements Runnable
   {
      int count;

      ConfirmDelivery(int count)
      {
         this.count = count;
      }

      public void run()
      {
         if (trace) { log.trace("confirming delivery on client of " + count + " message(s)"); }
         consumerDelegate.confirmDelivery(count);
      }
   }
   
   public void copyState(MessageCallbackHandler newHandler)
   {
      synchronized (mainLock)
      {
         this.consumerID = newHandler.consumerID;
         
         this.consumerDelegate = newHandler.consumerDelegate;
         
         this.sessionDelegate = newHandler.sessionDelegate;
         
         this.serverSending = false;
         
         this.buffer.clear();
      }
   }

}




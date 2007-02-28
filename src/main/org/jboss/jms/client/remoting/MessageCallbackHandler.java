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
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.server.endpoint.Cancel;
import org.jboss.jms.server.endpoint.DefaultCancel;
import org.jboss.jms.server.endpoint.DeliveryInfo;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.util.Future;
import org.jboss.messaging.util.prioritylinkedlist.BasicPriorityLinkedList;
import org.jboss.messaging.util.prioritylinkedlist.PriorityLinkedList;

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
   // Constants ------------------------------------------------------------------------------------
   
   private static final Logger log;
   
   // Static ---------------------------------------------------------------------------------------
   
   private static boolean trace;      
   
   static
   {
      log = Logger.getLogger(MessageCallbackHandler.class);
      trace = log.isTraceEnabled();
   }
   
   private static boolean checkExpiredOrReachedMaxdeliveries(MessageProxy proxy,
                                                             SessionDelegate del,
                                                             int maxDeliveries)
   {
      Message msg = proxy.getMessage();
      
      boolean expired = msg.isExpired();
      
      boolean reachedMaxDeliveries = proxy.getDeliveryCount() == maxDeliveries;
      
      if (expired || reachedMaxDeliveries)
      {
         if (trace)
         {
            if (expired)
            {
               log.trace(proxy.getMessage() + " has expired, cancelling to server");
            }
            else
            {
               log.trace(proxy.getMessage() + " has reached maximum delivery number, cancelling to server");
            }
         }
         final Cancel cancel = new DefaultCancel(proxy.getDeliveryId(), proxy.getDeliveryCount(),
                                                 expired, reachedMaxDeliveries);
         
         try
         {
            del.cancelDelivery(cancel);
         }
         catch (JMSException e)
         {
            log.error("Failed to cancel delivery", e);
         }   
               
         return true;
      }
      else
      {
         return false;
      }
   }
     
   //This is static so it can be called by the asf layer too
   public static void callOnMessage(SessionDelegate sess,
                                    MessageListener listener,
                                    int consumerID,
                                    String queueName,
                                    boolean isConnectionConsumer,
                                    MessageProxy m,
                                    int ackMode,
                                    int maxDeliveries,
                                    SessionDelegate connectionConsumerSession)
      throws JMSException
   {      
      if (checkExpiredOrReachedMaxdeliveries(m, sess, maxDeliveries))
      {
         //Message has been cancelled
         return;
      }
      
      DeliveryInfo deliveryInfo =
         new DeliveryInfo(m, consumerID, queueName, connectionConsumerSession);
            
      m.incDeliveryCount();
      
      // If this is the callback-handler for a connection consumer we don't want to acknowledge or
      // add anything to the tx for this session.
      if (!isConnectionConsumer)
      {
         //We need to call preDeliver, deliver the message then call postDeliver - this is because
         //it is legal to call session.recover(), or session.rollback() from within the onMessage()
         //method in which case the last message needs to be delivered so it needs to know about it
         sess.preDeliver(deliveryInfo);
      } 
      
      try
      {
         if (trace) { log.trace("calling listener's onMessage(" + m + ")"); }
                     
         listener.onMessage(m);

         if (trace) { log.trace("listener's onMessage() finished"); }
      }
      catch (RuntimeException e)
      {
         long id = m.getMessage().getMessageID();

         log.error("RuntimeException was thrown from onMessage, " + id + " will be redelivered", e);
         
         // See JMS 1.1 spec 4.5.2

         if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE)
         {              
            sess.recover();
         }
      }   

      // If this is the callback-handler for a connection consumer we don't want to acknowledge
      // or add anything to the tx for this session
      if (!isConnectionConsumer)
      {
         sess.postDeliver();
      }   
   }
   
   // Attributes -----------------------------------------------------------------------------------
      
   /*
    * The buffer is now a priority linked list
    * This resolves problems whereby messages are delivered from the server side queue in
    * correct priority order, but because the old consumer list was not a priority list
    * then if messages were sitting waiting to be consumed on the client side, then higher
    * priority messages might be behind lower priority messages and thus get consumed out of order
    */
   private PriorityLinkedList buffer;
   private SessionDelegate sessionDelegate;
   private ConsumerDelegate consumerDelegate;
   private int consumerID;
   private boolean isConnectionConsumer;
   private volatile Thread receiverThread;
   private MessageListener listener;
   private int ackMode;
   private boolean closed;
   private Object mainLock;
   private int maxBufferSize;
   private int minBufferSize;
   private QueuedExecutor sessionExecutor;
   private boolean listenerRunning;
   private int maxDeliveries;
   private String queueName;
   private long lastDeliveryId = -1;
   private volatile boolean serverSending = true;
   
        
   // Constructors ---------------------------------------------------------------------------------

   public MessageCallbackHandler(boolean isCC, int ackMode,                                
                                 SessionDelegate sess, ConsumerDelegate cons, int consumerID,
                                 String queueName,
                                 int bufferSize, QueuedExecutor sessionExecutor,
                                 int maxDeliveries)
   {
      if (bufferSize < 1)
      {
         throw new IllegalArgumentException(this + " bufferSize must be > 0");
      }
              
      this.maxBufferSize = bufferSize;
      this.minBufferSize = bufferSize / 2;
      buffer = new BasicPriorityLinkedList(10);
      isConnectionConsumer = isCC;
      this.ackMode = ackMode;
      this.sessionDelegate = sess;
      this.consumerDelegate = cons;
      this.consumerID = consumerID;
      this.queueName = queueName;
      mainLock = new Object();
      this.sessionExecutor = sessionExecutor;
      this.maxDeliveries = maxDeliveries;
   }
        
   // Public ---------------------------------------------------------------------------------------

   /**
    * Handles a message sent from the server.
    *
    * @param message The message
    */
   public void handleMessage(final Object message) throws Exception
   {
      //TODO - we temporarily need to execute on a different thread to avoid a deadlock situation in
      //       failover where a message is sent then the valve is locked, and the message send cause
      //       a message delivery back to the same client which tries to ack but can't get through
      //       the valve. This won't be necessary when we move to a non blocking transport
      this.sessionExecutor.execute(
         new Runnable()
         {
            public void run()
            {
               try
               {
                  handleMessageInternal(message);
               }
               catch (Exception e)
               {
                  log.error("Failed to handle message", e);
               }
            }
         });
   }
   
   public void setMessageListener(MessageListener listener) throws JMSException
   {     
      synchronized (mainLock)
      {
         if (receiverThread != null)
         {
            // Should never happen
            throw new IllegalStateException("Consumer is currently in receive(..). " +
               "Cannot set MessageListener");
         }
         
         this.listener = listener;
                            
         if (listener != null && !buffer.isEmpty())
         {  
            listenerRunning = true;
            
            this.queueRunner(new ListenerRunner());
         }        
      }   
   }
   
   public void cancelBuffer() throws JMSException
   {
      synchronized (mainLock)
      {      
         // Now we cancel anything left in the buffer. The reason we do this now is that otherwise
         // the deliveries wouldn't get cancelled until session close (since we don't cancel
         // consumer's deliveries until then), which is too late - since we need to preserve the
         // order of messages delivered in a session.
         
         if (!buffer.isEmpty())
         {                        
            // Now we cancel any deliveries that might be waiting in our buffer. This is because
            // otherwise the messages wouldn't get cancelled until the corresponding session died.
            // So if another consumer in another session tried to consume from the channel before
            // that session died it wouldn't receive those messages.
            // We can't just cancel all the messages in the SCE since some of those messages might
            // have actually been delivered (unlike these) and we may want to acknowledge them
            // later, after this consumer has been closed
   
            List cancels = new ArrayList();
   
            for(Iterator i = buffer.iterator(); i.hasNext();)
            {
               MessageProxy mp = (MessageProxy)i.next();
               
               DefaultCancel ack =
                  new DefaultCancel(mp.getDeliveryId(), mp.getDeliveryCount(), false, false);
               
               cancels.add(ack);
            }
                  
            sessionDelegate.cancelDeliveries(cancels);
            
            buffer.clear();
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
      
      if (trace) { log.trace(this + " closed"); }
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
            throw new JMSException("The consumer has a MessageListener set, " +
               "cannot call receive(..)");
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
                       
               boolean ignore =
                  checkExpiredOrReachedMaxdeliveries(m, sessionDelegate, maxDeliveries);
               
               if (!isConnectionConsumer && !ignore)
               {
                  DeliveryInfo info = new DeliveryInfo(m, consumerID, queueName, null);
                                                    
                  m.incDeliveryCount();           
 
                  sessionDelegate.preDeliver(info);                  
                  
                  sessionDelegate.postDeliver();                                    
               }
                                             
               if (!ignore)
               {
                  if (trace) { log.trace(this + ": message " + m + " is not expired, pushing it to the caller"); }
                  
                  break;
               }
               
               if (trace)
               {
                  log.trace(this + ": message expired or exceeded max deliveries, discarding");
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

      checkStart();
      
      if (trace) { log.trace(this + " receive() returning " + m); }
      
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
       this.consumerID = consumerId;
   }
   
   public void addToFrontOfBuffer(MessageProxy proxy) throws Exception
   {
      synchronized (mainLock)
      {
         buffer.addFirst(proxy, proxy.getJMSPriority());
         
         messageAdded();
      }
   }

   /**
    * Needed for failover
    */
   public void synchronizeWith(MessageCallbackHandler newHandler, QueuedExecutor sessionExecutor)
   {
      consumerID = newHandler.consumerID;

      // Clear the buffer. This way the non persistent messages that managed to arive are
      // irremendiably lost, while the peristent ones are failed-over on the server and will be
      // resent

      // TODO If we don't zap this buffer, we may be able to salvage some non-persistent messages

      buffer.clear();
      
      this.sessionExecutor = sessionExecutor;
      
      // need to reset toggle state
      serverSending = true;
      
   }
   
   public long getLastDeliveryId()
   {
      synchronized (mainLock)
      {
         return lastDeliveryId;
      }
   }
     
   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
            
   // Private --------------------------------------------------------------------------------------

   private void handleMessageInternal(Object message) throws Exception
   {
      MessageProxy proxy = (MessageProxy) message;

      if (trace) { log.trace(this + " receiving message " + proxy + " from the remoting layer"); }

      synchronized (mainLock)
      {
         if (closed)
         {
            // Ignore
            if (trace) { log.trace(this + " is closed, so ignore message"); }
            return;
         }

         proxy.setSessionDelegate(sessionDelegate, isConnectionConsumer);

         //Add it to the buffer
         buffer.addLast(proxy, proxy.getJMSPriority());

         lastDeliveryId = proxy.getDeliveryId();

         if (trace) { log.trace(this + " added message(s) to the buffer"); }

         messageAdded();

         checkStop();
      }
   }

   private void checkStop()
   {
      int size = buffer.size();
      
      if (serverSending && size >= maxBufferSize)
      {
         //Our buffer is full - we need to tell the server to stop sending if we haven't
         //done so already
         
         sendChangeRateMessage(0f);
         
         if (trace) { log.trace("Sent changeRate 0 message"); }
         
         serverSending = false;
      }
   }
   
   private void checkStart()
   {
      int size = buffer.size();
      
      if (!serverSending && size <= minBufferSize)
      {
         //We need more messages - we need to tell the server this if we haven't done so already
         
         sendChangeRateMessage(1.0f);
         
         if (trace) { log.trace("Sent changeRate 1.0 message"); }
         
         serverSending = true;
      }      
   }
   
   private void sendChangeRateMessage(float newRate) 
   {
      try
      {
         // this invocation will be sent asynchronously to the server; it's DelegateSupport.invoke()
         // job to detect it and turn it into a remoting one way invocation.
         consumerDelegate.changeRate(newRate);
      }
      catch (JMSException e)
      {
         log.error("Failed to send changeRate message", e);
      }
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
   
   private void messageAdded()
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
   
   private long waitOnLock(Object lock, long waitTime) throws InterruptedException
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
        
   private MessageProxy getMessage(long timeout)
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

      return m;
   }
   
   // Inner classes --------------------------------------------------------------------------------
         
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
               callOnMessage(sessionDelegate, listener, consumerID, queueName,
                             false, mp, ackMode, maxDeliveries, null);
            }
            catch (JMSException e)
            {
               log.error("Failed to deliver message", e);
            } 
         }
                  
         checkStart();
         
         if (again)
         {
            // Queue it up again
            queueRunner(this);
         }                                               
      }
   }   
}




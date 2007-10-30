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
package org.jboss.jms.client.container;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.jms.delegate.Cancel;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.DefaultCancel;
import org.jboss.jms.delegate.DeliveryInfo;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.MessageProxy;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.util.Future;
import org.jboss.messaging.util.prioritylinkedlist.BasicPriorityLinkedList;
import org.jboss.messaging.util.prioritylinkedlist.PriorityLinkedList;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox/a>
 * @version <tt>$Revision: 2774 $</tt>
 *
 * $Id: MessageCallbackHandler.java 2774 2007-06-12 22:43:54Z timfox $
 */
public class ClientConsumer
{
   // Constants ------------------------------------------------------------------------------------
   
   private static final Logger log;
   
   // Static ---------------------------------------------------------------------------------------
   
   private static boolean trace;      
   
   private static final int WAIT_TIMEOUT = 30000;
   
   
   static
   {
      log = Logger.getLogger(ClientConsumer.class);
      trace = log.isTraceEnabled();
   }
   
   private static boolean checkExpiredOrReachedMaxdeliveries(MessageProxy proxy,
                                                             SessionDelegate del,
                                                             int maxDeliveries, boolean shouldCancel)
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
               log.trace(proxy.getMessage() + " has reached maximum delivery number " + maxDeliveries +", cancelling to server");
            }
         }
         
         if (shouldCancel)
         {	         
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
                                    String consumerID,
                                    String queueName,
                                    boolean isConnectionConsumer,
                                    MessageProxy m,
                                    int ackMode,
                                    int maxDeliveries,
                                    SessionDelegate connectionConsumerSession,
                                    boolean shouldAck)
      throws JMSException
   {      
      if (checkExpiredOrReachedMaxdeliveries(m, connectionConsumerSession!=null?connectionConsumerSession:sess, maxDeliveries, shouldAck))
      {
         //Message has been cancelled
         return;
      }
      
      DeliveryInfo deliveryInfo =
         new DeliveryInfo(m, consumerID, queueName, connectionConsumerSession, shouldAck);
            
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
      	if (trace) { log.trace("Calling postDeliver"); }
      	
         sess.postDeliver();
         
         if (trace) { log.trace("Called postDeliver"); }
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
   private String consumerID;
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
   private boolean waitingForLastDelivery;
   private boolean shouldAck;
   private boolean handleFlowControl;
   private long redeliveryDelay;
   private volatile int currentToken;
   
   // Constructors ---------------------------------------------------------------------------------

   public ClientConsumer(boolean isCC, int ackMode,                                
                         SessionDelegate sess, ConsumerDelegate cons, String consumerID,
                         String queueName,
                         int bufferSize, QueuedExecutor sessionExecutor,
                         int maxDeliveries, boolean shouldAck, boolean handleFlowControl,
                         long redeliveryDelay)
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
      this.shouldAck = shouldAck;
      this.handleFlowControl = handleFlowControl;
      this.redeliveryDelay = redeliveryDelay;
   }
        
   // Public ---------------------------------------------------------------------------------------


   public boolean isClosed()
   {
      return closed;
   }

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
   	
      sessionExecutor.execute(new HandleMessageRunnable(currentToken, message));         
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
   	if (trace) { log.trace("Cancelling buffer: " + buffer.size()); }
   	
      synchronized (mainLock)
      {      
         // Now we cancel anything left in the buffer. The reason we do this now is that otherwise
         // the deliveries wouldn't get cancelled until session close (since we don't cancel
         // consumer's deliveries until then), which is too late - since we need to preserve the
         // order of messages delivered in a session.
         
         if (shouldAck && !buffer.isEmpty())
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
               
               DefaultCancel cancel =
                  new DefaultCancel(mp.getDeliveryId(), mp.getDeliveryCount(), false, false);
               
               cancels.add(cancel);
            }
                  
            if (trace) { log.trace("Calling cancelDeliveries"); }
            sessionDelegate.cancelDeliveries(cancels);
            if (trace) { log.trace("Done call"); }
            
            buffer.clear();
         }    
      }
   }
   
   public void close(long lastDeliveryId) throws JMSException
   {     
   	log.trace(this + " close");
         	
   	//Wait for the last delivery to arrive
      waitForLastDelivery(lastDeliveryId);
      
      //Important! We set the listener to null so the next ListenerRunner won't run
      if (listener != null)
      {
      	setMessageListener(null);
      }
      
      //Now we wait for any current listener runners to run.
      waitForOnMessageToComplete();   
      
      synchronized (mainLock)
      {         
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
                  checkExpiredOrReachedMaxdeliveries(m, sessionDelegate, maxDeliveries, shouldAck);
               
               if (!isConnectionConsumer && !ignore)
               {
                  DeliveryInfo info = new DeliveryInfo(m, consumerID, queueName, null, shouldAck);
                                                    
                  sessionDelegate.preDeliver(info);                  
                  
                  //If post deliver didn't succeed and acknowledgement mode is auto_ack
                  //That means the ref wasn't acked since it couldn't be found.
                  //In order to maintain at most once semantics we must therefore not return
                  //the message
                  
                  ignore = !sessionDelegate.postDeliver();  
                  
                  if (trace)
                  {
                  	log.trace("Post deliver returned " + !ignore);
                  }
                  
                  if (!ignore)
                  {
                     m.incDeliveryCount();                                
                  }
               }
                                             
               if (!ignore)
               {
                  if (trace) { log.trace(this + ": message " + m + " is not expired, pushing it to the caller"); }
                  
                  break;
               }
               
               if (trace)
               {
                  log.trace("Discarding message " + m);
               }
               
               // the message expired, so discard the message, adjust timeout and reenter the buffer
               if (timeout != 0)
               {
                  timeout -= System.currentTimeMillis() - startTimestamp;
                  if (timeout == 0)
                  {
                     // As 0 means waitForever, we make it noWait
                     timeout = -1;
                  }

               }
            }           
         }
         finally
         {
            receiverThread = null;            
         }
      } 
      
      //This needs to be outside the lock

      if (handleFlowControl)
      {
      	checkStart();
      }
      
      if (trace) { log.trace(this + " receive() returning " + m); }
      
      return m;
   } 
         
   public MessageListener getMessageListener()
   {
      return listener;      
   }

   public String toString()
   {
      return "ClientConsumer[" + consumerID + "]";
   }
   
   public String getConsumerId()
   {
      return consumerID;
   }

   public void setConsumerId(String consumerId)
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
    * Note this can't lock the mainLock since receive() also locks the main lock
    * and this would prevent failover occuring when a consumer is blocked on receive()
    */
   public void synchronizeWith(ClientConsumer newHandler)
   {
      currentToken++;
   	
      consumerID = newHandler.consumerID;

      // Clear the buffer. This way the non persistent messages that managed to arrive are
      // irredeemably lost, while the persistent ones are failed-over on the server and will be
      // resent

      buffer.clear();
      
      // need to reset toggle state
      serverSending = true;
   }
   
   public long getRedeliveryDelay()
   {
   	return redeliveryDelay;
   }
   
   
   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
            
   // Private --------------------------------------------------------------------------------------

   /*
    * Wait for the last delivery to arrive
    */
   private void waitForLastDelivery(long id)
   {
      if (trace) { log.trace("Waiting for last delivery id " + id); }
      
      if (id == -1)
      {
      	//No need to wait - nothing to wait for      	
      	return;
      }
      
      synchronized (mainLock)
      {          
         waitingForLastDelivery = true;
         try
         {
            long wait = WAIT_TIMEOUT;
            while (lastDeliveryId != id && wait > 0)
            {
               long start = System.currentTimeMillis();  
               try
               {
                  mainLock.wait(wait);
               }
               catch (InterruptedException e)
               {               
               }
               wait -= (System.currentTimeMillis() - start);
            }      
            if (trace && lastDeliveryId == id)
            {
               log.trace("Got last delivery");
            }
             
            if (lastDeliveryId != id)
            {
               log.warn("Timed out waiting for last delivery " + id + " got " + lastDeliveryId); 
            }
         }
         finally
         {
            waitingForLastDelivery = false;
         }
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
         // the current thread already closing this ClientConsumer (this happens when the
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
      }
   }
   
   private void messageAdded()
   {
      boolean notified = false;
      
      if (trace) { log.trace("Receiver thread:" + receiverThread + " listener:" + listener + " listenerRunning:" + listenerRunning + 
      		" sessionExecutor:" + sessionExecutor); }
      
      // If we have a thread waiting on receive() we notify it
      if (receiverThread != null)
      {
         if (trace) { log.trace(this + " notifying receiver/waiter thread"); }   
         
         mainLock.notifyAll();
         
         notified = true;
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
      
      // Make sure we notify any thread waiting for last delivery
      if (waitingForLastDelivery && !notified)
      {
      	if (trace) { log.trace("Notifying"); }
      	
         mainLock.notifyAll();
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
   
   private class HandleMessageRunnable implements Runnable
   {
   	private int token;
   	
   	private Object message;
   	
   	HandleMessageRunnable(int token, Object message)
   	{
   		this.token = token;
   		
   		this.message = message;
   	}
   	
   	public void run()
      {
         try
         {
         	 MessageProxy proxy = (MessageProxy) message;

             if (trace) { log.trace(this + " receiving message " + proxy + " from the remoting layer"); }

             synchronized (mainLock)
             {
                if (closed)
                {
                   // Sanity - this should never happen - we should always wait for all deliveries to arrive
                   // when closing
                   throw new IllegalStateException(this + " is closed, so ignoring message");
                }
                
                if (token != currentToken)
                {
               	 //This message was queued up from before failover - we don't want to add it
               	 log.trace("Ignoring message " + message);
               	 return;
                }
                
                proxy.setSessionDelegate(sessionDelegate, isConnectionConsumer);

                proxy.getMessage().doBeforeReceive();

                //Add it to the buffer
                buffer.addLast(proxy, proxy.getJMSPriority());

                lastDeliveryId = proxy.getDeliveryId();
                
                if (trace) { log.trace(this + " added message(s) to the buffer are now " + buffer.size() + " messages"); }

                messageAdded();

                if (handleFlowControl)
                {
                	checkStop();
                }
             }
         }
         catch (Exception e)
         {
            log.error("Failed to handle message", e);
         }
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
         
         MessageListener theListener = null;
         
         synchronized (mainLock)
         {
            if (listener == null || buffer.isEmpty())
            {
               listenerRunning = false;
               
               if (trace) { log.trace("no listener or buffer is empty, returning"); }
               
               return;
            }
            
            theListener = listener;
            
            // remove a message from the buffer

            mp = (MessageProxy)buffer.removeFirst();                                       
         }
         
         /*
          * Bug here is as follows:
          * The next runner gets scheduled BEFORE the on message is executed
          * so if the onmessage fails on acking it will be put on hold
          * and failover will kick in, this will clear the executor
          * so the next queud one disappears at everything grinds to a halt
          * 
          * Solution - don't use a session executor - have a sesion thread instead much nicer
          */
                                
         if (mp != null)
         {
            try
            {
               callOnMessage(sessionDelegate, theListener, consumerID, queueName,
                             false, mp, ackMode, maxDeliveries, null, shouldAck);
               
               if (trace) { log.trace("Called callonMessage"); }
            }
            catch (Throwable t)
            {
               log.error("Failed to deliver message", t);
            } 
         }
         
         synchronized (mainLock)
         {
         	if (!buffer.isEmpty())
            {
            	//Queue up the next runner to run
            	
            	if (trace) { log.trace("More messages in buffer so queueing next onMessage to run"); }
            	
            	queueRunner(this);
            	
            	if (trace) { log.trace("Queued next onMessage to run"); }
            }
            else
            {
            	if (trace) { log.trace("no more messages in buffer, marking listener as not running"); }
            	
            	listenerRunning  = false;
            }   
         }
                  
         if (handleFlowControl)
         {
         	checkStart();                                                   
         }
         
         if (trace) { log.trace("Exiting run()"); }
      }
   }   
}




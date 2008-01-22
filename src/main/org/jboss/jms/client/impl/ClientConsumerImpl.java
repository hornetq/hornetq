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
package org.jboss.jms.client.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.jms.client.api.ClientConsumer;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.exception.MessagingShutdownException;
import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.PriorityLinkedList;
import org.jboss.messaging.core.impl.PriorityLinkedListImpl;
import org.jboss.messaging.core.remoting.Client;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.wireformat.ChangeRateMessage;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;
import org.jboss.messaging.util.Future;
import org.jboss.messaging.util.Logger;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * The client-side ClientConsumer delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @version <tt>$Revision: 3603 $</tt>
 *
 * $Id: ClientConsumerImpl.java 3603 2008-01-21 18:49:20Z timfox $
 */
public class ClientConsumerImpl extends CommunicationSupport implements ClientConsumer
{
   // Constants ------------------------------------------------------------------------------------

	private static final long serialVersionUID = 3253922610778321868L;

	private static final Logger log = Logger.getLogger(ClientConsumerImpl.class);
	
	private static final boolean trace = log.isTraceEnabled();
	
	private static final int WAIT_TIMEOUT = 30000;
      

   // Attributes -----------------------------------------------------------------------------------

	private ClientSession session;
   private int bufferSize;
   private int maxDeliveries;
   private long redeliveryDelay;
   private Destination destination;
   private String selector;
   private String subscriptionName;
   private boolean noLocal;
   private boolean isConnectionConsumer;
   private boolean storingDeliveries;
   
   private PriorityLinkedList<JBossMessage> buffer = new PriorityLinkedListImpl<JBossMessage>(10);
   private volatile Thread receiverThread;
   private MessageListener listener;
   private int ackMode;
   private boolean closed;
   private Object mainLock = new Object();
   private QueuedExecutor sessionExecutor;
   private boolean listenerRunning;
   private long lastDeliveryId = -1;
   private boolean waitingForLastDelivery;
   private boolean shouldAck;
   private boolean paused;      
   private int consumeCount;
   private boolean firstTime = true;

   
   // Static ---------------------------------------------------------------------------------------
   
   private static boolean checkExpiredOrReachedMaxdeliveries(JBossMessage jbm,
         ClientSession del,
         int maxDeliveries, boolean shouldCancel)
   {
      Message msg = jbm.getCoreMessage();

      boolean expired = msg.isExpired();

      boolean reachedMaxDeliveries = jbm.getDeliveryCount() == maxDeliveries;

      if (expired || reachedMaxDeliveries)
      {
         if (trace)
         {
            if (expired)
            {
               log.trace(msg + " has expired, cancelling to server");
            }
            else
            {
               log.trace(msg + " has reached maximum delivery number " + maxDeliveries +", cancelling to server");
            }
         }

         if (shouldCancel)
         {           
            final Cancel cancel = new CancelImpl(jbm.getDeliveryId(), jbm.getDeliveryCount(),
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

   // This is static so it can be called by the asf layer too
   public static void callOnMessage(ClientSession sess,
         MessageListener listener,
         String consumerID,
         boolean isConnectionConsumer,
         JBossMessage m,
         int ackMode,
         int maxDeliveries,
         ClientSession connectionConsumerSession,
         boolean shouldAck)
   throws JMSException
   {      
      if (checkExpiredOrReachedMaxdeliveries(m, connectionConsumerSession!=null?connectionConsumerSession:sess, maxDeliveries, shouldAck))
      {
         // Message has been cancelled
         return;
      }

      DeliveryInfo deliveryInfo =
         new DeliveryInfo(m, consumerID, connectionConsumerSession, shouldAck);

      m.incDeliveryCount();

      // If this is the callback-handler for a connection consumer we don't want to acknowledge or
      // add anything to the tx for this session.
      if (!isConnectionConsumer)
      {
         // We need to call preDeliver, deliver the message then call postDeliver - this is because
         // it is legal to call session.recover(), or session.rollback() from within the onMessage()
         // method in which case the last message needs to be delivered so it needs to know about it
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
         log.error("RuntimeException was thrown from onMessage, " + m.getJMSMessageID() + " will be redelivered", e);

         // See JMS 1.1 spec 4.5.2

         if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE)
         {              
            sess.recover();
         }
      }   

      // If this is the callback-handler for a connection consumer we don't want to acknowledge
      //or add anything to the tx for this session
      if (!isConnectionConsumer)
      {
         if (trace) { log.trace("Calling postDeliver"); }

         sess.postDeliver();

         if (trace) { log.trace("Called postDeliver"); }
      }   
   }

   // Constructors ---------------------------------------------------------------------------------

   public ClientConsumerImpl(ClientSession session, String objectID, int bufferSize,
                             int maxDeliveries, long redeliveryDelay,
                             Destination dest,
                             String selector, boolean noLocal, String subscriptionName,
                             boolean isCC, QueuedExecutor sessionExecutor)
   {
      super(objectID);
      this.session = session;
      this.bufferSize = bufferSize;
      this.maxDeliveries = maxDeliveries;
      this.redeliveryDelay = redeliveryDelay;
      this.destination = dest;
      this.selector = selector;
      this.noLocal = noLocal;
      this.subscriptionName = subscriptionName;
      this.isConnectionConsumer = isCC;
      this.sessionExecutor = sessionExecutor;
      this.shouldAck = !(destination.getType() == DestinationType.TOPIC && subscriptionName == null);  
   }

   @Override
   protected byte getVersion()
   {
      return session.getConnection().getVersion();
   }

   protected Client getClient()
   {
      return this.session.getConnection().getClient();
   }
   
   // Closeable implementation ---------------------------------------------------------------------

   public void close() throws JMSException
   {
      try
      {
         sendBlocking(new CloseMessage());
      }
      finally
      {
         session.removeChild(this.getID());
      }
   }


   public long closing(long sequence) throws JMSException
   {
      try
      {
         // We make sure closing is called on the ServerConsumerEndpoint.
         // This returns us the last delivery id sent

         long lastDeliveryId = invokeClosing(sequence);

         // First we call close on the ClientConsumer which waits for onMessage invocations
         // to complete and the last delivery to arrive
         close(lastDeliveryId);

         PacketDispatcher.client.unregister(id);

         //And then we cancel any messages still in the message callback handler buffer
         cancelBuffer();

         return lastDeliveryId;

      }
      catch (Exception proxiedException)
      {
         // if MessagingServer is shutdown or
         // if there is no failover in place... we just close the consumerState as well
         if (proxiedException instanceof MessagingShutdownException /* ||
                 (connectionState.getFailoverCommandCenter() == null ) */ )

         {
            if (!this.isClosed())
            {
               close(-1);
            }
         }
         JMSException ex = new JMSException(proxiedException.toString());
         ex.initCause(proxiedException);
         throw ex;
      }
   }

   private long invokeClosing(long sequence) throws JMSException
   {
      ClosingRequest request = new ClosingRequest(sequence);
      ClosingResponse response = (ClosingResponse) sendBlocking(request);
      return response.getID();
   }

   public boolean isClosed()
   {
      return closed;
   }
   
   public void changeRate(float newRate) throws JMSException
   {
      sendOneWay(new ChangeRateMessage(newRate));
   }
   
   public MessageListener getMessageListener()
   {
      return this.listener;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public boolean getNoLocal()
   {
      return this.noLocal;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public Destination getDestination()
   {
      return this.destination;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public String getMessageSelector()
   {
      return this.selector;
   }

   // Streamable implementation ----------------------------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);

      bufferSize = in.readInt();

      maxDeliveries = in.readInt();
      
      redeliveryDelay = in.readLong();
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);

      out.writeInt(bufferSize);

      out.writeInt(maxDeliveries);
      
      out.writeLong(redeliveryDelay);
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ConsumerDelegate[" + System.identityHashCode(this) + ", ID=" + id + "]";
   }

   public int getBufferSize()
   {
      return bufferSize;
   }

   public int getMaxDeliveries()
   {
      return maxDeliveries;
   }
   
   public long getRedeliveryDelay()
   {
   	return redeliveryDelay;
   }
   
   public boolean isConnectionConsumer()
   {
      return isConnectionConsumer;
   }
 
   public String getSubscriptionName()
   {
      return subscriptionName;
   }

   public void setSubscriptionName(String subscriptionName)
   {
      this.subscriptionName = subscriptionName;
   }

   public boolean isStoringDeliveries()
   {
      return storingDeliveries;
   }
   
   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner Classes --------------------------------------------------------------------------------
         
   public void handleMessage(final JBossMessage message) throws Exception
   {
      synchronized (mainLock)
      {
         if (closed)
         {
            // Sanity - this should never happen - we should always wait for all deliveries to arrive
            // when closing
            throw new IllegalStateException(this + " is closed, so ignoring message");
         }

         message.setSessionDelegate(session, isConnectionConsumer);

         message.doBeforeReceive();

         //Add it to the buffer
         buffer.addLast(message, message.getJMSPriority());

         lastDeliveryId = message.getDeliveryId();

         if (trace) { log.trace(this + " added message(s) to the buffer are now " + buffer.size() + " messages"); }

         messageAdded();
      }
   }

   public void setMessageListener(MessageListener listener) throws JMSException
   {     
      synchronized (mainLock)
      {
         if (receiverThread != null)
         {
            // Should never happen
            throw new IllegalStateException("ClientConsumer is currently in receive(..). " +
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
               JBossMessage mp = (JBossMessage)i.next();
               
               CancelImpl cancel =
                  new CancelImpl(mp.getDeliveryId(), mp.getDeliveryCount(), false, false);
               
               cancels.add(cancel);
            }
                  
            if (trace) { log.trace("Calling cancelDeliveries"); }
            session.cancelDeliveries(cancels);
            if (trace) { log.trace("Done call"); }
            
            buffer.clear();
         }    
      }
   }
   
   private void close(long lastDeliveryId) throws JMSException
   {     
      try
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
      finally
      {
         session.removeChild(this.getID());
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
   public JBossMessage receive(long timeout) throws JMSException
   {                
      JBossMessage m = null;      
      
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
                  checkExpiredOrReachedMaxdeliveries(m, session, maxDeliveries, shouldAck);
               
               if (!isConnectionConsumer && !ignore)
               {
                  DeliveryInfo info = new DeliveryInfo(m, id, null, shouldAck);
                                                    
                  session.preDeliver(info);                  
                  
                  //If post deliver didn't succeed and acknowledgement mode is auto_ack
                  //That means the ref wasn't acked since it couldn't be found.
                  //In order to maintain at most once semantics we must therefore not return
                  //the message
                  
                  ignore = !session.postDeliver();  
                  
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
      
      if (trace) { log.trace(this + " receive() returning " + m); }
      
      return m;
   } 
         
   public void addToFrontOfBuffer(JBossMessage proxy) throws JMSException
   {
      synchronized (mainLock)
      {
         buffer.addFirst(proxy, proxy.getJMSPriority());
         
         consumeCount--;
         
         messageAdded();
      }
   }

   public void pause()
   {
      synchronized (mainLock)
      {
         paused = true;

         sendChangeRateMessage(0f);         
      }
   }

   public void resume()
   {
      synchronized (mainLock)
      {
         paused = false;

         if (firstTime)
         {
            consumeCount = 0;

            firstTime = false;
         }
         else
         {
            consumeCount = bufferSize / 3 - buffer.size();
         }

         sendChangeRateMessage(1f);
      }
   }
   
   public boolean isShouldAck()
   {
      return this.shouldAck;
   }

   
   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
            
   // Private --------------------------------------------------------------------------------------

   private void checkSendChangeRate()
   {
      consumeCount++;
      
      if (!paused && consumeCount == bufferSize)
      {
         consumeCount = 0;

         sendChangeRateMessage(1.0f);
      }
   }

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
   
   private void sendChangeRateMessage(float newRate) 
   {
      try
      {
         // this invocation will be sent asynchronously to the server; it's DelegateSupport.invoke()
         // job to detect it and turn it into a remoting one way invocation.
         changeRate(newRate);
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
        
   private JBossMessage getMessage(long timeout)
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

      JBossMessage m = null;
             
      if (!closed && !buffer.isEmpty())
      {
         m = (JBossMessage)buffer.removeFirst();
         
         checkSendChangeRate();
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
         JBossMessage msg = null;
         
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

            msg = (JBossMessage)buffer.removeFirst();                
            
            checkSendChangeRate();
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
                                
         if (msg != null)
         {
            try
            {
               callOnMessage(session, theListener, id,
                             false, msg, ackMode, maxDeliveries, null, shouldAck);
               
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
                  
         if (trace) { log.trace("Exiting run()"); }
      }
   }   
   

}

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

import org.jboss.jms.client.api.MessageHandler;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.PriorityLinkedList;
import org.jboss.messaging.core.impl.PriorityLinkedListImpl;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ConsumerChangeRateMessage;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.util.Future;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @version <tt>$Revision: 3603 $</tt>
 *
 * $Id: ClientConsumerImpl.java 3603 2008-01-21 18:49:20Z timfox $
 */
public class ClientConsumerImpl implements ClientConsumerInternal
{
   // Constants ------------------------------------------------------------------------------------

	private static final Logger log = Logger.getLogger(ClientConsumerImpl.class);
	
	private static final boolean trace = log.isTraceEnabled();
	
   // Attributes -----------------------------------------------------------------------------------

	private String id;
	private ClientSessionInternal session;
   private int bufferSize;
   private PriorityLinkedList<DeliverMessage> buffer = new PriorityLinkedListImpl<DeliverMessage>(10);
   private volatile Thread receiverThread;
   private MessageHandler handler;
   private volatile boolean closed;
   private Object mainLock = new Object();
   private QueuedExecutor sessionExecutor;
   private boolean listenerRunning;
   private int consumeCount;
   private MessagingRemotingConnection remotingConnection;
   private String queueName;
   private long ignoreDeliveryMark = -1;
   
   //FIXME - revisit closed and closing flags
   
   // Static ---------------------------------------------------------------------------------------
      
   // Constructors ---------------------------------------------------------------------------------

   public ClientConsumerImpl(ClientSessionInternal session, String id, int bufferSize,
                             QueuedExecutor sessionExecutor,
                             MessagingRemotingConnection remotingConnection,
                             String queueName)
   {
      this.id = id;
      this.session = session;
      this.bufferSize = bufferSize;
      this.sessionExecutor = sessionExecutor;
      this.remotingConnection = remotingConnection;    
      this.queueName = queueName;
   }

   // ClientConsumer implementation -----------------------------------------------------------------
      
   public Message receive(long timeout) throws MessagingException
   {      
      checkClosed();
      
      DeliverMessage m = null;      
      
      synchronized (mainLock)
      {        
         if (trace) { log.trace(this + " receiving, timeout = " + timeout); }
         
         if (closed)
         {
            if (trace) { log.trace(this + " closed, returning null"); }
            return null;
         }
         
         if (handler != null)
         {
            throw new MessagingException(MessagingException.ILLEGAL_STATE, "Cannot call receive(...) - a MessageHandler is set");
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
                       
               boolean expired = m.getMessage().isExpired();
               
               session.delivered(m.getDeliveryID(), expired);
               
               if (!expired)
               {
                  break;
               }

               if (trace) { log.trace("Message has expired " + m); }
               
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
         
      return m.getMessage();
   } 
   
   public Message receiveImmediate() throws MessagingException
   { 
      return receive(-1);
   }
   
   public MessageHandler getMessageHandler() throws MessagingException
   {
      checkClosed();
      
      return handler;
   }
   
   public void setMessageHandler(MessageHandler handler) throws MessagingException
   {  
      checkClosed();
      
      synchronized (mainLock)
      {
         if (receiverThread != null)
         {
            throw new MessagingException(MessagingException.ILLEGAL_STATE, "Cannot set MessageHandler - consumer is in receive(...)");
         }
         
         this.handler = handler;
                            
         if (handler != null && !buffer.isEmpty())
         {  
            listenerRunning = true;
            
            this.queueRunner(new ListenerRunner());
         }        
      }   
   }
   
   public String getQueueName()
   {
      return queueName;
   }
      
   public synchronized void close() throws MessagingException
   {
      if (closed)
      {
         return;
      }
               
      try
      {
         //Important! We set the handler to null so the next ListenerRunner won't run
         if (handler != null)
         {
            setMessageHandler(null);
         }
         
         //Now we wait for any current handler runners to run.
         waitForOnMessageToComplete();   
         
         //TODO sort out these close and closing flags
         
         synchronized (mainLock)
         {                     
            closed = true;
                       
            if (receiverThread != null)
            {            
               // Wake up any receive() thread that might be waiting
               mainLock.notify();
            }   
            
            this.handler = null;
         }
         
         remotingConnection.send(id, new CloseMessage());
         
         PacketDispatcher.client.unregister(id);
                                      
         if (trace) { log.trace(this + " closed"); }
                  
      }
      finally
      {
         session.removeConsumer(this);
      }

   }

   public boolean isClosed()
   {
      return closed;
   }

   // ClientConsumerInternal implementation --------------------------------------------------------------
   
   public String getID()
   {
      return id;
   }
      
   public void changeRate(float newRate) throws MessagingException
   {
      checkClosed();
      
      remotingConnection.send(id, new ConsumerChangeRateMessage(newRate), true);
   }
   
   public void handleMessage(final DeliverMessage message) throws Exception
   {
      synchronized (mainLock)
      {
         if (closed)
         {
            //This is ok - we just ignore the message
            return;
         }
                  
         if (ignoreDeliveryMark >= 0)
         {
            long delID = message.getDeliveryID();
            
            if (delID > ignoreDeliveryMark)
            {
               //Ignore - the session is recovering and these are inflight messages
               return;
            }
            else
            {
               //We have hit the begining of the recovered messages - we can stop ignoring
               ignoreDeliveryMark = -1;
            }            
         }
         
         //Add it to the buffer    
         Message coreMessage = message.getMessage();
         
         coreMessage.setDeliveryCount(message.getDeliveryCount());
         
         buffer.addLast(message, coreMessage.getPriority());

         if (trace) { log.trace(this + " added message(s) to the buffer are now " + buffer.size() + " messages"); }

         messageAdded();
      }
   }
   
   public void recover(long lastDeliveryID)
   {
      synchronized (mainLock)
      {
         ignoreDeliveryMark = lastDeliveryID;
         
         buffer.clear();
      }            
   }
   
   // Public ---------------------------------------------------------------------------------------
     
   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
            
   // Private --------------------------------------------------------------------------------------
   
   private void checkSendChangeRate() throws MessagingException
   {
      consumeCount++;
      
      if (consumeCount == bufferSize)
      {
         consumeCount = 0;

         changeRate(1.0f);
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

         result.getResult();
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
      
      if (trace) { log.trace("Receiver thread:" + receiverThread + " handler:" + handler + " listenerRunning:" + listenerRunning + 
            " sessionExecutor:" + sessionExecutor); }
      
      // If we have a thread waiting on receive() we notify it
      if (receiverThread != null)
      {
         if (trace) { log.trace(this + " notifying receiver/waiter thread"); }   
         
         mainLock.notifyAll();
         
         notified = true;
      }     
      else if (handler != null)
      { 
         // We have a message handler
         if (!listenerRunning)
         {
            listenerRunning = true;

            if (trace) { log.trace(this + " scheduled a new ListenerRunner"); }
            
            this.queueRunner(new ListenerRunner());
         }     
         
         //TODO - Execute onMessage on same thread for even better throughput 
      }
      
      // Make sure we notify any thread waiting for last delivery
      if (!notified)
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
        
   private DeliverMessage getMessage(long timeout) throws MessagingException
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

      DeliverMessage m = null;
             
      if (!closed && !buffer.isEmpty())
      {
         m = buffer.removeFirst();
         
         checkSendChangeRate();
      }

      return m;
   }
   
   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Consumer is closed");
      }
   }

   
   // Inner classes --------------------------------------------------------------------------------
         
   /*
    * This class is used to put on the handler executor to wait for onMessage
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
         try
         {
            DeliverMessage msg = null;
            
            MessageHandler theListener = null;
            
            synchronized (mainLock)
            {
               if (handler == null || buffer.isEmpty())
               {
                  listenerRunning = false;
                  
                  if (trace) { log.trace("no handler or buffer is empty, returning"); }
                  
                  return;
               }
               
               theListener = handler;
               
               // remove a message from the buffer
   
               msg = buffer.removeFirst();                
               
               checkSendChangeRate();
            }
            
            /*
             * Bug here is as follows:
             * The next runner gets scheduled BEFORE the on message is executed
             * so if the onmessage fails on acking it will be put on hold
             * and failover will kick in, this will clear the executor
             * so the next queud one disappears at everything grinds to a halt
             * 
             * Solution - don't use a session executor - have a session thread instead much nicer
             */
                                   
            if (msg != null)
            {     
               boolean expired = msg.getMessage().isExpired();
                           
               session.delivered(msg.getDeliveryID(), expired);
               
               if (!expired)
               {
                  theListener.onMessage(msg.getMessage());
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
                  if (trace) { log.trace("no more messages in buffer, marking handler as not running"); }
                  
                  listenerRunning  = false;
               }   
            }
                     
            if (trace) { log.trace("Exiting run()"); }
         }
         catch (MessagingException e)
         {
            log.error("Failure in ListenerRunner", e);
         }
      }      
   }   
  
}

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.client.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.list.PriorityLinkedList;
import org.jboss.messaging.core.list.impl.PriorityLinkedListImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionFlowCreditMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * 
 * @version <tt>$Revision: 3603 $</tt>
 * 
 * $Id: ClientConsumerImpl.java 3603 2008-01-21 18:49:20Z timfox $
 */
public class ClientConsumerImpl implements ClientConsumerInternal
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientConsumerImpl.class);

   private static final boolean trace = log.isTraceEnabled();
   
   public static final long CLOSE_TIMEOUT_MILLISECONDS = 10000;

   // Attributes
   // -----------------------------------------------------------------------------------

   private final ClientSessionInternal session;
   
   private final Channel channel;
      
   private final int id;
   
   private final Executor sessionExecutor;
   
   private final int clientWindowSize;
   
   private final PriorityLinkedList<ClientMessage> buffer = new PriorityLinkedListImpl<ClientMessage>(10);
   
   private final boolean direct;
   
   private final Runner runner = new Runner();
      
   private volatile Thread receiverThread;
   
   private volatile Thread onMessageThread;
      
   private volatile MessageHandler handler;
   
   private volatile boolean closed;
      
   private volatile long ignoreDeliveryMark = -1;
   
   private volatile int creditsToSend;   
   
  
   // Constructors
   // ---------------------------------------------------------------------------------

   public ClientConsumerImpl(final ClientSessionInternal session,
                             final int id,                                                             
                             final int clientWindowSize,
                             final boolean direct,                             
                             final Executor executor,
                             final Channel channel)
   {
      this.id = id;
      
      this.channel = channel;
      
      this.session = session;
      
      this.sessionExecutor = executor;
      
      this.clientWindowSize = clientWindowSize;
      
      this.direct = direct;
   }

   // ClientConsumer implementation
   // -----------------------------------------------------------------
      
   public synchronized ClientMessage receive(long timeout) throws MessagingException
   {
      checkClosed();

      if (handler != null)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE, "Cannot call receive(...) - a MessageHandler is set");
      }

      receiverThread = Thread.currentThread();

      if (timeout == 0)            
      {
         //Effectively infinite
         timeout = Long.MAX_VALUE;
      }
      
      long start = System.currentTimeMillis();
      
      long toWait = timeout;

      try
      {
         while (true)
         {
            while (!closed && buffer.isEmpty() && toWait > 0)
            {
               try
               {
                  wait(toWait);
               }
               catch (InterruptedException e)
               {
               }
               
               //TODO - can avoid this extra System.currentTimeMillis call by exiting early
               long now = System.currentTimeMillis();
               
               toWait -= now - start;
               
               start = now;
            }
                    
            if (!closed && !buffer.isEmpty())
            {                              
               ClientMessage m = buffer.removeFirst();
               
               boolean expired = m.isExpired();
               
               session.delivered(m.getDeliveryID(), expired);
               
               flowControl(m.getEncodeSize());
                                 
               if (expired)
               {
                  if (toWait > 0)
                  {
                     continue;
                  }
                  else
                  {
                     return null;
                  }
               }
                                 
               return m;
            }
            else
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

   public ClientMessage receive() throws MessagingException
   {
   	return receive(0);
   }
   
   public ClientMessage receiveImmediate() throws MessagingException
   {
      return receive(-1);
   }

   public MessageHandler getMessageHandler() throws MessagingException
   {
      checkClosed();

      return handler;
   }

   public void setMessageHandler(final MessageHandler handler) throws MessagingException
   {
      checkClosed();
      
      if (receiverThread != null)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE,"Cannot set MessageHandler - consumer is in receive(...)");
      }
        
   	waitForOnMessageToComplete();   	
   	
      this.handler = handler;
      
      //If there are any messages in the buffer, we need to queue up executors for them
      synchronized (this)
      {
         for (int i = 0; i < buffer.size(); i++)
         {      
            queueExecutor();
         }
      }
   }
   
   public void close() throws MessagingException
   {
      doCleanUp(true);
   }

   public synchronized void cleanUp()
   {
      try
      {
         doCleanUp(false);
      }
      catch (MessagingException e)
      {
         log.warn("problem cleaning up: " + this);
      }
   }

   public boolean isClosed()
   {
      return closed;
   }
   
   public boolean isDirect()
   {
      return direct;
   }

   // ClientConsumerInternal implementation
   // --------------------------------------------------------------
   
   public int getID()
   {
      return id;
   }

   public void handleMessage(final ClientMessage message) throws Exception
   {    
      if (closed)
      {
         // This is ok - we just ignore the message
         return;
      }
      
      if (ignoreDeliveryMark >= 0)
      {
         long delID = message.getDeliveryID();
         
         if (delID > ignoreDeliveryMark)
         {
            // Ignore - the session is recovering and these are inflight
            // messages
            return;
         }
         else if (delID == ignoreDeliveryMark)
         {
            // We have hit the begining of the recovered messages - we can
            // stop ignoring
            ignoreDeliveryMark = -1;
         }
         else
         {
         	throw new IllegalStateException("Invalid delivery id " + delID);
         }
      }
                  
      if (handler != null)
      {
         if (direct)
         {
            //Dispatch it directly on remoting thread
            
            boolean expired = message.isExpired();

            session.delivered(message.getDeliveryID(), expired);
            
            flowControl(message.getEncodeSize());

            if (!expired)
            {
               handler.onMessage(message);
            }
         }
         else
         {
         	//Execute using executor
            
         	synchronized (this)
         	{   
         		buffer.addLast(message, message.getPriority());         		
         	}
         	
         	queueExecutor();
         }
      }
      else
      {
      	 // Add it to the buffer
      	synchronized (this)
      	{      	         	   
      		buffer.addLast(message, message.getPriority());
         	      		
      		notify();
      	}
      }      
   }

   public void recover(final long lastDeliveryID)
   {
      ignoreDeliveryMark = lastDeliveryID;

      buffer.clear();      
   }
   
   public int getClientWindowSize()
   {
      return clientWindowSize;
   }
   
   public long getIgnoreDeliveryMark()
   {
      return ignoreDeliveryMark;
   }
   
   public int getBufferSize()
   {
      return buffer.size();
   }
   
   public int getCreditsToSend()
   {
      return creditsToSend;
   }

   // Public
   // ---------------------------------------------------------------------------------------

   // Package protected
   // ----------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void queueExecutor()
   {
      sessionExecutor.execute(runner);
   }
   
   private void flowControl(final int messageBytes) throws MessagingException
   {
      if (clientWindowSize > 0)
      {
         creditsToSend += messageBytes;
         
         if (creditsToSend >= clientWindowSize)
         {            
            channel.send(new SessionFlowCreditMessage(id, creditsToSend));
            
            creditsToSend = 0;            
         }
      }
   }
   
   private void waitForOnMessageToComplete()
   {
   	if (handler == null)
   	{
   		return;
   	}
   	
      if (Thread.currentThread() == onMessageThread)
      {
         // If called from inside onMessage then return immediately - otherwise would block
         return;
      }
      
      Future future = new Future();

      sessionExecutor.execute(future);
      
      boolean ok = future.await(CLOSE_TIMEOUT_MILLISECONDS);

      if (!ok)
      {
      	log.warn("Timed out waiting for handler to complete processing");
      }
   }
     
   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Consumer is closed");
      }
   }
   
   private void callOnMessage()
   {
   	try
		{
   		if (closed)
   		{
   			return;
   		}
   		
   		//We pull the message from the buffer from inside the Runnable so we can ensure priority
   		//ordering. If we just added a Runnable with the message to the executor immediately as we get it
   		//we could not do that
   		
   		ClientMessage message;
   		
   		synchronized (this)
   		{
   		   message = buffer.removeFirst();
   		}
   		
   		if (message != null)
   		{   		      		
      		boolean expired = message.isExpired();
   
            session.delivered(message.getDeliveryID(), expired);
            
            flowControl(message.getEncodeSize());
   
            if (!expired)
            {
         		onMessageThread = Thread.currentThread();
         		
               handler.onMessage(message);
            }
   		}
		}
		catch (MessagingException e)
		{
			log.error("Failed to execute", e);
		}
		catch (RuntimeException e)
		{
			log.error("RuntimeException thrown from handler", e);
		}
   }

   private void doCleanUp(final boolean sendCloseMessage) throws MessagingException
   {
      if (closed)
      {
         return;
      }

      try
      {
         // Now we wait for any current handler runners to run.
         waitForOnMessageToComplete();
    
         closed = true;

         if (receiverThread != null)
         {
            synchronized (this)
            {
               // Wake up any receive() thread that might be waiting
               notify();
            }
         }

         handler = null;

         receiverThread = null;

         if (sendCloseMessage)
         {
            channel.sendBlocking(new SessionConsumerCloseMessage(id));
         }
      }
      finally
      {
         session.removeConsumer(this);
      }
   }

   // Inner classes
   // --------------------------------------------------------------------------------
   
   private class Runner implements Runnable
   {
      public void run()
      {
         callOnMessage();
      } 
   }
   
   private static class Future implements Runnable
   {
      private CountDownLatch latch = new CountDownLatch(1);
      
      public boolean await(long timeout)
      {
         try
         {
            return latch.await(timeout, TimeUnit.MILLISECONDS);
         }
         catch (Exception e)
         {
            return false;
         }
      }
      
      public void run()
      {
         latch.countDown();
      }
   }
   

}

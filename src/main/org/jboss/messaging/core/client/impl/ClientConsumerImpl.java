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
package org.jboss.messaging.core.client.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.PriorityLinkedList;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.impl.PriorityLinkedListImpl;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

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
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientConsumerImpl.class);

   private static final boolean trace = log.isTraceEnabled();
   
   private static final long CLOSE_TIMEOUT_SECONDS = 10;

   // Attributes
   // -----------------------------------------------------------------------------------

   private final ClientSessionInternal session;
      
   private final String id;
   
   private final ExecutorService sessionExecutor;
   
   private final RemotingConnection remotingConnection;
   
   private final boolean direct;
   
   private final int tokenBatchSize;
   
   private final PriorityLinkedList<DeliverMessage> buffer = new PriorityLinkedListImpl<DeliverMessage>(10);
   
   private volatile Thread receiverThread;
   
   private volatile Thread onMessageThread;
      
   private volatile MessageHandler handler;
   
   private volatile boolean closed;
      
   private volatile long ignoreDeliveryMark = -1;
   
   private volatile int tokensToSend;   

   // Constructors
   // ---------------------------------------------------------------------------------

   public ClientConsumerImpl(final ClientSessionInternal session, final String id,
                             final ExecutorService sessionExecutor,
                             final RemotingConnection remotingConnection,
                             final boolean direct, final int tokenBatchSize)
   {
      this.id = id;
      
      this.session = session;
      
      this.sessionExecutor = sessionExecutor;
      
      this.remotingConnection = remotingConnection;
      
      this.direct = direct;
      
      this.tokenBatchSize = tokenBatchSize;
   }

   // ClientConsumer implementation
   // -----------------------------------------------------------------

   public synchronized Message receive(long timeout) throws MessagingException
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
               
               long now = System.currentTimeMillis();
               
               toWait -= now - start;
               
               start = now;
            }
                    
            if (!closed && !buffer.isEmpty())
            {                              
               DeliverMessage m = buffer.removeFirst();
               
               boolean expired = m.getMessage().isExpired();
               
               session.delivered(m.getDeliveryID(), expired);
               
               flowControl();
                                 
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
                                 
               return m.getMessage();
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
      
      if (receiverThread != null)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE,"Cannot set MessageHandler - consumer is in receive(...)");
      }
        
   	waitForOnMessageToComplete();   	
   	
      this.handler = handler;      
   }

   public void close() throws MessagingException
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

         remotingConnection.send(id, new CloseMessage());

         PacketDispatcher.client.unregister(id);
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

   // ClientConsumerInternal implementation
   // --------------------------------------------------------------

   public String getID()
   {
      return id;
   }

   public void handleMessage(final DeliverMessage message) throws Exception
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
         else
         {
            // We have hit the begining of the recovered messages - we can
            // stop ignoring
            ignoreDeliveryMark = -1;
         }
      }
      
      if (handler != null)
      {
         if (direct)
         {
            //Dispatch it directly on remoting thread
            
            boolean expired = message.getMessage().isExpired();

            session.delivered(message.getDeliveryID(), expired);
            
            flowControl();

            if (!expired)
            {
               handler.onMessage(message.getMessage());
            }
         }
         else
         {
         	//Execute using executor
         	
         	synchronized (this)
         	{
         		buffer.addLast(message, message.getMessage().getPriority());
         	}
         	            	
         	sessionExecutor.execute(new Runnable() { public void run() { callOnMessage(); } } );
         }
      }
      else
      {
      	 // Add it to the buffer
      	
      	synchronized (this)
      	{
      		buffer.addLast(message, message.getMessage().getPriority());
         	      		
      		notify();
      	}
      }      
   }

   public void recover(long lastDeliveryID)
   {
      ignoreDeliveryMark = lastDeliveryID;

      buffer.clear();      
   }

   // Public
   // ---------------------------------------------------------------------------------------

   // Package protected
   // ----------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void flowControl() throws MessagingException
   {
      if (tokenBatchSize > 0)
      {
         tokensToSend++;
   
         if (tokensToSend == tokenBatchSize)
         {
            tokensToSend = 0;
            
            remotingConnection.send(id, new ConsumerFlowTokenMessage(tokenBatchSize), true);                  
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

      Future<?> future = sessionExecutor.submit(new Runnable() { public void run() {} });

      long start = System.currentTimeMillis();
      try
      {
      	future.get(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
      catch (Exception e)
      {
      	//Ignore
      }
      long end = System.currentTimeMillis();
      
      if (end - start >= CLOSE_TIMEOUT_SECONDS * 1000)
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
   		
   		DeliverMessage message;
   		
   		synchronized (this)
   		{
   		   message = buffer.removeFirst();
   		}
   		
   		if (message != null)
   		{      		
      		boolean expired = message.getMessage().isExpired();
   
            session.delivered(message.getDeliveryID(), expired);
            
            flowControl();
   
            if (!expired)
            {
         		onMessageThread = Thread.currentThread();
         		
               handler.onMessage(message.getMessage());
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
   
   // Inner classes
   // --------------------------------------------------------------------------------

}

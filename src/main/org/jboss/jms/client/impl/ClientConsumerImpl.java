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

import java.util.concurrent.ExecutorService;

import org.jboss.jms.client.api.MessageHandler;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.PriorityLinkedList;
import org.jboss.messaging.core.impl.PriorityLinkedListImpl;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.util.Future;
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

   // Attributes
   // -----------------------------------------------------------------------------------

   private String id;
   
   private ClientSessionInternal session;
   
   private PriorityLinkedList<DeliverMessage> buffer = new PriorityLinkedListImpl<DeliverMessage>(10);
   
   private volatile Thread receiverThread;
   
   private MessageHandler handler;
   
   private volatile boolean closed;
   
   private Object mainLock = new Object();
   
   private ExecutorService sessionExecutor;
   
   private boolean listenerRunning;
   
   private MessagingRemotingConnection remotingConnection;
   
   private long ignoreDeliveryMark = -1;
   
   private boolean direct;
   
   private Thread onMessageThread;
   
   private int tokensToSend;
   
   private int tokenBatchSize;
   
   // Static
   // ---------------------------------------------------------------------------------------

   // Constructors
   // ---------------------------------------------------------------------------------

   public ClientConsumerImpl(ClientSessionInternal session, String id,
                             ExecutorService sessionExecutor,
                             MessagingRemotingConnection remotingConnection,
                             boolean direct, int tokenBatchSize)
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

   public Message receive(long timeout) throws MessagingException
   {
      checkClosed();

      synchronized (mainLock)
      {
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
                     mainLock.wait(toWait);
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

      synchronized (mainLock)
      {         
         this.handler = handler;

         if (handler != null && !buffer.isEmpty())
         {
            listenerRunning = true;

            queueRunner();
         }
      }
   }

   public void close() throws MessagingException
   {
      if (closed)
      {
         return;
      }

      try
      {
         // We set the handler to null so the next ListenerRunner won't run
         handler = null;

         // Now we wait for any current handler runners to run.
         waitForOnMessageToComplete();

         synchronized (mainLock)
         {
            closed = true;

            if (receiverThread != null)
            {
               // Wake up any receive() thread that might be waiting
               mainLock.notify();
            }
         }

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

//   public void changeRate(float newRate) throws MessagingException
//   {
//      checkClosed();
//
//      remotingConnection.send(id, new ConsumerFlowTokenMessage(newRate), true);
//   }

   public void handleMessage(final DeliverMessage message) throws Exception
   {
      synchronized (mainLock)
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

         // Add it to the buffer
         Message coreMessage = message.getMessage();

         coreMessage.setDeliveryCount(message.getDeliveryCount());

         buffer.addLast(message, coreMessage.getPriority());

         if (receiverThread != null)
         {
            mainLock.notify();
         }
         else if (handler != null)
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
            else if (!listenerRunning)
            {
               listenerRunning = true;

               queueRunner();
            }
         }
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
      // Wait for any onMessage() executions to complete

      if (Thread.currentThread() == onMessageThread)
      {
         // If called from inside onMessage then return immediately - otherwise would block forever
         return;
      }

      Future result = new Future();

      sessionExecutor.execute(new Closer(result));

      result.getResult();
   }

   private void queueRunner()
   {
      sessionExecutor.execute(new ListenerRunner());
   }

   private void checkClosed() throws MessagingException
   {
      if (closed)
      {
         throw new MessagingException(MessagingException.OBJECT_CLOSED, "Consumer is closed");
      }
   }
   
   private void onMessageLoop()
   {
      try
      {
         onMessageThread = Thread.currentThread();

         DeliverMessage msg = null;

         MessageHandler theListener = null;
                  
         synchronized (mainLock)
         {
            if (handler == null || buffer.isEmpty())
            {
               listenerRunning = false;

               return;
            }

            theListener = handler;

            msg = buffer.removeFirst();              
         }

         if (msg != null)
         {
            boolean expired = msg.getMessage().isExpired();

            session.delivered(msg.getDeliveryID(), expired);
            
            flowControl();

            if (!expired)
            {
               theListener.onMessage(msg.getMessage());
            }
         }

         synchronized (mainLock)
         {
            if (!buffer.isEmpty())
            {
               queueRunner();
            }
            else
            {
               listenerRunning = false;
            }
         }
      }
      catch (MessagingException e)
      {
         log.error("Failure in ListenerRunner", e);
      }
   }
   
   // Inner classes
   // --------------------------------------------------------------------------------

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
     
   private class ListenerRunner implements Runnable
   {
      public void run()
      {
         onMessageLoop();
      }
   }
}

/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.client.impl;

import java.io.File;
import java.util.concurrent.Executor;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.list.PriorityLinkedList;
import org.jboss.messaging.core.list.impl.PriorityLinkedListImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.utils.Future;
import org.jboss.messaging.utils.TokenBucketLimiter;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision: 3603 $</tt> $Id: ClientConsumerImpl.java 3603 2008-01-21 18:49:20Z timfox $
 */
public class ClientConsumerImpl implements ClientConsumerInternal
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientConsumerImpl.class);

   private static final boolean trace = log.isTraceEnabled();

   public static final long CLOSE_TIMEOUT_MILLISECONDS = 10000;

   public static final int NUM_PRIORITIES = 10;

   // Attributes
   // -----------------------------------------------------------------------------------

   private final ClientSessionInternal session;

   private final Channel channel;

   private final long id;

   private final Executor sessionExecutor;

   private final int clientWindowSize;

   private final int ackBatchSize;

   private final PriorityLinkedList<ClientMessageInternal> buffer = new PriorityLinkedListImpl<ClientMessageInternal>(NUM_PRIORITIES);

   private final Runner runner = new Runner();

   private final File directory;

   private ClientMessageInternal currentChunkMessage;
   
   private LargeMessageBufferImpl currentLargeMessageBuffer;
   
   // When receiving LargeMessages, the user may choose to not read the body, on this case we need to discard te body before moving to the next message.
   private ClientMessageInternal largeMessageReceived;

   private final TokenBucketLimiter rateLimiter;

   private volatile Thread receiverThread;

   private volatile Thread onMessageThread;

   private volatile MessageHandler handler;

   private volatile boolean closing;

   private volatile boolean closed;

   private volatile int creditsToSend;
   
   private volatile boolean slowConsumerInitialCreditSent = false;

   private volatile Exception lastException;

   private volatile int ackBytes;

   private volatile ClientMessage lastAckedMessage;

   private boolean stopped = false;

   // Constructors
   // ---------------------------------------------------------------------------------

   public ClientConsumerImpl(final ClientSessionInternal session,
                             final long id,
                             final int clientWindowSize,
                             final int ackBatchSize,
                             final TokenBucketLimiter rateLimiter,
                             final Executor executor,
                             final Channel channel,
                             final File directory)
   {
      this.id = id;

      this.channel = channel;

      this.session = session;

      this.rateLimiter = rateLimiter;

      sessionExecutor = executor;

      this.clientWindowSize = clientWindowSize;

      this.ackBatchSize = ackBatchSize;

      this.directory = directory;
   }

   // ClientConsumer implementation
   // -----------------------------------------------------------------

   public ClientMessage receive(long timeout) throws MessagingException
   {
      checkClosed();
      
      if (largeMessageReceived != null)
      {
         // Check if there are pending packets to be received
         largeMessageReceived.discardLargeBody();
         largeMessageReceived = null;
      }

      if (rateLimiter != null)
      {
         rateLimiter.limit();
      }

      if (handler != null)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE,
                                      "Cannot call receive(...) - a MessageHandler is set");
      }
      
      if (clientWindowSize == 0)
      {
         startSlowConsumer();
      }

      receiverThread = Thread.currentThread();

      if (timeout == 0)
      {
         // Effectively infinite
         timeout = Long.MAX_VALUE;
      }

      long start = -1;

      long toWait = timeout;

      try
      {
         while (true)
         {
            ClientMessageInternal m = null;

            synchronized (this)
            {
               while ((stopped || (m = buffer.removeFirst()) == null) && !closed && toWait > 0)

               {
                  if (start == -1)
                  {
                     start = System.currentTimeMillis();
                  }

                  try
                  {
                     wait(toWait);
                  }
                  catch (InterruptedException e)
                  {
                  }

                  if (m != null || closed)
                  {
                     break;
                  }

                  long now = System.currentTimeMillis();

                  toWait -= now - start;

                  start = now;
               }
            }

            if (m != null)
            {
               boolean expired = m.isExpired();

               flowControlBeforeConsumption(m);

               if (expired)
               {
                  m.discardLargeBody();
                  
                  session.expire(id, m.getMessageID());

                  if (clientWindowSize == 0)
                  {
                     startSlowConsumer();
                  }                 

                  if (toWait > 0)
                  {
                     continue;
                  }
                  else
                  {
                     return null;
                  }
               }

               if (m.isLargeMessage())
               {
                  this.largeMessageReceived = m;
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

   // Must be synchronized since messages may be arriving while handler is being set and might otherwise end
   // up not queueing enough executors - so messages get stranded
   public synchronized void setMessageHandler(final MessageHandler theHandler) throws MessagingException
   {
      checkClosed();

      if (receiverThread != null)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE,
                                      "Cannot set MessageHandler - consumer is in receive(...)");
      }

      boolean noPreviousHandler = handler == null;

      if (handler != theHandler && clientWindowSize == 0)
      {
         startSlowConsumer();
      }

      handler = theHandler;

      // if no previous handler existed queue up messages for delivery
      if (handler != null && noPreviousHandler)
      {

         requeueExecutors();
      }
      // if unsetting a previous handler may be in onMessage so wait for completion
      else if (handler == null && !noPreviousHandler)
      {
         waitForOnMessageToComplete();
      }
   }

   public void close() throws MessagingException
   {
      doCleanUp(true);
   }

   public void cleanUp()
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

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.client.ClientConsumer#isLargeMessagesAsFiles()
    */
   public boolean isFileConsumer()
   {
      return directory != null;
   }

   public void stop() throws MessagingException
   {
      waitForOnMessageToComplete();

      synchronized (this)
      {
         if (stopped)
         {
            return;
         }

         stopped = true;
      }
   }

   public synchronized void start()
   {
      stopped = false;

      requeueExecutors();
   }

   public Exception getLastException()
   {
      return lastException;
   }

   // ClientConsumerInternal implementation
   // --------------------------------------------------------------

   public long getID()
   {
      return id;
   }

   public synchronized void handleMessage(final ClientMessageInternal message) throws Exception
   {
      if (closing)
      {
         // This is ok - we just ignore the message
         return;
      }

      ClientMessageInternal messageToHandle = message;

      messageToHandle.onReceipt(this);
      
      if (trace)
      {
         log.trace("Adding message " + message + " into buffer");
      }

      // Add it to the buffer
      buffer.addLast(messageToHandle, messageToHandle.getPriority());

      if (handler != null)
      {
         // Execute using executor
         if (!stopped)
         {
            queueExecutor();
         }
      }
      else
      {
         notify();
      }
   }

   public synchronized void handleLargeMessage(final SessionReceiveMessage packet) throws Exception
   {
      if (closing)
      {
         // This is ok - we just ignore the message
         return;
      }
      
      // Flow control for the first packet, we will have others
      flowControl(packet.getPacketSize(), false);

      currentChunkMessage = new ClientMessageImpl();
      
      currentChunkMessage.decodeProperties(ChannelBuffers.wrappedBuffer(packet.getLargeMessageHeader()));
      
      currentChunkMessage.setLargeMessage(true);

      currentLargeMessageBuffer = new LargeMessageBufferImpl(this, packet.getLargeMessageSize(), 60);

      currentChunkMessage.setBody(currentLargeMessageBuffer);

      currentChunkMessage.setFlowControlSize(0);

      handleMessage(currentChunkMessage);
   }

   public synchronized void handleLargeMessageContinuation(final SessionReceiveContinuationMessage chunk) throws Exception
   {
      if (closing)
      {
         return;
      }
      
      currentLargeMessageBuffer.addPacket(chunk);
   }

   public void clear()
   {
      synchronized (this)
      {
         buffer.clear();
      }

      waitForOnMessageToComplete();
   }

   public int getClientWindowSize()
   {
      return clientWindowSize;
   }

   public int getBufferSize()
   {
      return buffer.size();
   }

   public void acknowledge(final ClientMessage message) throws MessagingException
   {
      ackBytes += message.getEncodeSize();

      if (ackBytes >= ackBatchSize)
      {
         doAck(message);
      }
      else
      {
         lastAckedMessage = message;
      }
   }

   public void flushAcks() throws MessagingException
   {
      if (lastAckedMessage != null)
      {
         doAck(lastAckedMessage);
      }
   }

   /** 
    * flow control is synchornized because of LargeMessage and streaming.
    * LargeMessageBuffer will call flowcontrol here, while other handleMessage will also be calling flowControl.
    * So, this operation needs to be atomic.
    * 
    * @parameter discountSlowConsumer When dealing with slowConsumers, we need to discount one credit that was pre-sent when the first receive was called. For largeMessage that is only done at the latest packet
    * */
   public void flowControl(final int messageBytes, final boolean discountSlowConsumer) throws MessagingException
   {
      if (clientWindowSize >= 0)
      {
         creditsToSend += messageBytes;
         
         if (creditsToSend >= clientWindowSize)
         {
            
            if (clientWindowSize == 0 && discountSlowConsumer)
            {
               if (trace)
               {
                  log.trace("Sending " + creditsToSend + " -1, for slow consumer");
               }
               
               slowConsumerInitialCreditSent = false;

               // sending the credits - 1 initially send to fire the slow consumer, or the slow consumer would be
               // always buffering one after received the first message
               final int credits = creditsToSend - 1;

               creditsToSend = 0;
               
               sendCredits(credits);
            }
            else
            {
               if (trace)
               {
                  log.trace("Sending " + messageBytes + " from flow-control");
               }
             
               final int credits = creditsToSend;

               creditsToSend = 0;

               sendCredits(credits);
            }
         }
      }
   }

   // Public
   // ---------------------------------------------------------------------------------------

   // Package protected
   // ---------------------------------------------------------------------------------------

   // Protected
   // ---------------------------------------------------------------------------------------

   // Private
   // ---------------------------------------------------------------------------------------

   /** 
    * Sending a initial credit for slow consumers
    * */
   private void startSlowConsumer()
   {
      if (!slowConsumerInitialCreditSent)
      {
         if (trace)
         {
            log.trace("Sending 1 credit to start delivering of one message to slow consumer");
         }
         slowConsumerInitialCreditSent = true;
         sendCredits(1);
      }
   }

   private void requeueExecutors()
   {
      for (int i = 0; i < buffer.size(); i++)
      {
         queueExecutor();
      }
   }

   private void queueExecutor()
   {
      if (trace)
      {
         log.trace("Adding Runner on Executor for delivery");
      }
      sessionExecutor.execute(runner);
   }

   /**
    * @param credits
    */
   private void sendCredits(final int credits)
   {
      if (trace)
      {
         log.trace("Sending " + credits + " credits back", new Exception ("trace"));
      }
      channel.send(new SessionConsumerFlowCreditMessage(id, credits));
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

      org.jboss.messaging.utils.Future future = new Future();

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

   private void callOnMessage() throws Exception
   {
      if (closing || stopped)
      {
         return;
      }
      // We pull the message from the buffer from inside the Runnable so we can ensure priority
      // ordering. If we just added a Runnable with the message to the executor immediately as we get it
      // we could not do that

      ClientMessageInternal message;

      // Must store handler in local variable since might get set to null
      // otherwise while this is executing and give NPE when calling onMessage
      MessageHandler theHandler = handler;

      if (theHandler != null)
      {
         if (rateLimiter != null)
         {
            rateLimiter.limit();
         }

         synchronized (this)
         {
            message = buffer.removeFirst();
         }

         if (message != null)
         {
            boolean expired = message.isExpired();

            flowControlBeforeConsumption(message);

            if (!expired)
            {
               onMessageThread = Thread.currentThread();

               if (trace)
               {
                  log.trace("Calling handler.onMessage");
               }
               theHandler.onMessage(message);
               
               if (trace)
               {
                  log.trace("Handler.onMessage done");
               }
               
               if (message.isLargeMessage())
               {
                  message.discardLargeBody();
               }
            }
            else
            {
               session.expire(id, message.getMessageID());
            }

            // If slow consumer, we need to send 1 credit to make sure we get another message
            if (clientWindowSize == 0)
            {
               startSlowConsumer();
            }
         }
      }
   }

   /**
    * @param message
    * @throws MessagingException
    */
   private void flowControlBeforeConsumption(final ClientMessageInternal message) throws MessagingException
   {
      // Chunk messages will execute the flow control while receiving the chunks
      if (message.getFlowControlSize() != 0)
      {
         flowControl(message.getFlowControlSize(), true);
      }
   }

   private void doCleanUp(final boolean sendCloseMessage) throws MessagingException
   {
      try
      {
         if (closed)
         {
            return;
         }

         // We need an extra flag closing, since we need to prevent any more messages getting queued to execute
         // after this and we can't just set the closed flag to true here, since after/in onmessage the message
         // might be acked and if the consumer is already closed, the ack will be ignored
         closing = true;

         // Now we wait for any current handler runners to run.
         waitForOnMessageToComplete();
         
         if (currentLargeMessageBuffer != null)
         {
            currentLargeMessageBuffer.close();
            currentLargeMessageBuffer = null;
         }
         

         closed = true;

         synchronized (this)
         {
            if (receiverThread != null)
            {
               // Wake up any receive() thread that might be waiting
               notify();
            }

            handler = null;

            receiverThread = null;
         }

         flushAcks();

         if (sendCloseMessage)
         {
            channel.sendBlocking(new SessionConsumerCloseMessage(id));
         }

         clearBuffer();
      }
      finally
      {
         session.removeConsumer(this);
      }
   }

   private void clearBuffer()
   {
      buffer.clear();
   }

   private void doAck(final ClientMessage message) throws MessagingException
   {
      ackBytes = 0;

      lastAckedMessage = null;

      session.acknowledge(id, message.getMessageID());
   }

   // Inner classes
   // --------------------------------------------------------------------------------

   private class Runner implements Runnable
   {
      public void run()
      {

         try
         {
            callOnMessage();
         }
         catch (Exception e)
         {
            log.error("Failed to call onMessage()", e);

            lastException = e;
         }

      }
   }

}

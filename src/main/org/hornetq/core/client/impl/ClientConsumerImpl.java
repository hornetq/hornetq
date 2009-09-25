/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.client.impl;

import java.io.File;
import java.util.concurrent.Executor;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.list.PriorityLinkedList;
import org.hornetq.core.list.impl.PriorityLinkedListImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.hornetq.utils.Future;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TokenBucketLimiter;

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
   
   private final SimpleString filterString;
   
   private final SimpleString queueName;
   
   private boolean browseOnly;
   
   private final Executor sessionExecutor;

   private final int clientWindowSize;

   private final int ackBatchSize;

   private final PriorityLinkedList<ClientMessageInternal> buffer = new PriorityLinkedListImpl<ClientMessageInternal>(NUM_PRIORITIES);

   private final Runner runner = new Runner();

   private LargeMessageBufferImpl currentLargeMessageBuffer;

   // When receiving LargeMessages, the user may choose to not read the body, on this case we need to discard the body
   // before moving to the next message.
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
                             final SimpleString queueName,
                             final SimpleString filterString,
                             final boolean browseOnly,
                             final int clientWindowSize,
                             final int ackBatchSize,
                             final TokenBucketLimiter rateLimiter,
                             final Executor executor,
                             final Channel channel)
   {
      this.id = id;
      
      this.queueName = queueName;
      
      this.filterString = filterString;
      
      this.browseOnly = browseOnly;

      this.channel = channel;

      this.session = session;

      this.rateLimiter = rateLimiter;

      sessionExecutor = executor;

      this.clientWindowSize = clientWindowSize;

      this.ackBatchSize = ackBatchSize;
   }

   // ClientConsumer implementation
   // -----------------------------------------------------------------

   public ClientMessage receive(long timeout) throws HornetQException
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
         throw new HornetQException(HornetQException.ILLEGAL_STATE,
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
               // if we have already pre acked we cant expire
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

   public ClientMessage receive() throws HornetQException
   {
      return receive(0);
   }

   public ClientMessage receiveImmediate() throws HornetQException
   {
      return receive(-1);
   }

   public MessageHandler getMessageHandler() throws HornetQException
   {
      checkClosed();

      return handler;
   }

   // Must be synchronized since messages may be arriving while handler is being set and might otherwise end
   // up not queueing enough executors - so messages get stranded
   public synchronized void setMessageHandler(final MessageHandler theHandler) throws HornetQException
   {
      checkClosed();

      if (receiverThread != null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE,
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

   public void close() throws HornetQException
   {
      doCleanUp(true);
   }

   public void cleanUp()
   {
      try
      {
         doCleanUp(false);
      }
      catch (HornetQException e)
      {
         log.warn("problem cleaning up: " + this);
      }
   }

   public boolean isClosed()
   {
      return closed;
   }

   public void stop() throws HornetQException
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
   
   public void clearAtFailover()
   {
      clearBuffer();
      
      lastAckedMessage = null;
      
      creditsToSend = 0;
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
   
   public SimpleString getFilterString()
   {
      return filterString;
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public boolean isBrowseOnly()
   {
      return browseOnly;
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

      ClientMessageInternal currentChunkMessage = new ClientMessageImpl(packet.getDeliveryCount());

      currentChunkMessage.decodeProperties(ChannelBuffers.wrappedBuffer(packet.getLargeMessageHeader()));

      currentChunkMessage.setLargeMessage(true);

      File largeMessageCache = null;

      if (session.isCacheLargeMessageClient())
      {
         largeMessageCache = File.createTempFile("tmp-large-message-" + currentChunkMessage.getMessageID() + "-",
                                                 ".tmp");
         largeMessageCache.deleteOnExit();
      }

      currentLargeMessageBuffer = new LargeMessageBufferImpl(this, packet.getLargeMessageSize(), 60, largeMessageCache);

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

   public void clear() throws HornetQException
   {
      synchronized (this)
      {
         // Need to send credits for the messages in the buffer

         for (ClientMessageInternal message : this.buffer)
         {
            flowControlBeforeConsumption(message);
         }

         clearBuffer();
      }

      // Need to send credits for the messages in the buffer

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

   public void acknowledge(final ClientMessage message) throws HornetQException
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

   public void flushAcks() throws HornetQException
   {
      if (lastAckedMessage != null)
      {
         doAck(lastAckedMessage);
      }
   }

   /** 
    * 
    * LargeMessageBuffer will call flowcontrol here, while other handleMessage will also be calling flowControl.
    * So, this operation needs to be atomic.
    * 
    * @parameter discountSlowConsumer When dealing with slowConsumers, we need to discount one credit that was pre-sent when the first receive was called. For largeMessage that is only done at the latest packet
    * */
   public void flowControl(final int messageBytes, final boolean discountSlowConsumer) throws HornetQException
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

      org.hornetq.utils.Future future = new Future();

      sessionExecutor.execute(future);

      boolean ok = future.await(CLOSE_TIMEOUT_MILLISECONDS);

      if (!ok)
      {
         log.warn("Timed out waiting for handler to complete processing");
      }
   }

   private void checkClosed() throws HornetQException
   {
      if (closed)
      {
         throw new HornetQException(HornetQException.OBJECT_CLOSED, "Consumer is closed");
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
    * @throws HornetQException
    */
   private void flowControlBeforeConsumption(final ClientMessageInternal message) throws HornetQException
   {
      // Chunk messages will execute the flow control while receiving the chunks
      if (message.getFlowControlSize() != 0)
      {
         flowControl(message.getFlowControlSize(), true);
      }
   }

   private void doCleanUp(final boolean sendCloseMessage) throws HornetQException
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
            currentLargeMessageBuffer.cancel();
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

         clearBuffer();

         if (sendCloseMessage)
         {
            channel.sendBlocking(new SessionConsumerCloseMessage(id));
         }
      }
      catch (Throwable t)
      {
         // Consumer close should always return without exception
      }

      session.removeConsumer(this);
   }   

   private void clearBuffer()
   {
      buffer.clear();
   }

   private void doAck(final ClientMessage message) throws HornetQException
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

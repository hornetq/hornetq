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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.SessionConsumerCloseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.client.HornetQClientMessageBundle;
import org.hornetq.utils.FutureLatch;
import org.hornetq.utils.PriorityLinkedList;
import org.hornetq.utils.PriorityLinkedListImpl;
import org.hornetq.utils.ReusableLatch;
import org.hornetq.utils.TokenBucketLimiter;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision: 3603 $</tt> $Id: ClientConsumerImpl.java 3603 2008-01-21 18:49:20Z timfox $
 */
public final class ClientConsumerImpl implements ClientConsumerInternal
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final boolean isTrace = HornetQClientLogger.LOGGER.isTraceEnabled();

   private static final long CLOSE_TIMEOUT_MILLISECONDS = 10000;

   private static final int NUM_PRIORITIES = 10;

   public static final SimpleString FORCED_DELIVERY_MESSAGE = new SimpleString("_hornetq.forced.delivery.seq");

   // Attributes
   // -----------------------------------------------------------------------------------

   private final ClientSessionInternal session;

   private final Channel channel;

   private final long id;

   private final SimpleString filterString;

   private final SimpleString queueName;

   private final boolean browseOnly;

   private final Executor sessionExecutor;

   // For failover we can't send credits back
   // while holding a lock or failover could dead lock eventually
   // And we can't use the sessionExecutor as that's being used for message handlers
   // for that reason we have a separate flowControlExecutor that's using the thread pool
   // Which is a OrderedExecutor
   private final Executor flowControlExecutor;

   // Number of pending calls on flow control
   private final ReusableLatch pendingFlowControl = new ReusableLatch(0);

   private final int clientWindowSize;

   private final int ackBatchSize;

   private final PriorityLinkedList<ClientMessageInternal> buffer = new PriorityLinkedListImpl<ClientMessageInternal>(ClientConsumerImpl.NUM_PRIORITIES);

   private final Runner runner = new Runner();

   private LargeMessageControllerImpl currentLargeMessageController;

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

   private volatile boolean failedOver;

   private volatile Exception lastException;

   private volatile int ackBytes;

   private volatile ClientMessageInternal lastAckedMessage;

   private boolean stopped = false;

   private long forceDeliveryCount;

   private final SessionQueueQueryResponseMessage queueInfo;

   private volatile boolean ackIndividually;

   private final ClassLoader contextClassLoader;

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
                             final Executor flowControlExecutor,
                             final Channel channel,
                             final SessionQueueQueryResponseMessage queueInfo,
                             final ClassLoader contextClassLoader)
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

      this.queueInfo = queueInfo;

      this.contextClassLoader = contextClassLoader;

      this.flowControlExecutor = flowControlExecutor;
   }

   // ClientConsumer implementation
   // -----------------------------------------------------------------

   private ClientMessage receive(final long timeout, final boolean forcingDelivery) throws HornetQException
   {
      checkClosed();

      if (largeMessageReceived != null)
      {
         // Check if there are pending packets to be received
         largeMessageReceived.discardBody();
         largeMessageReceived = null;
      }

      if (rateLimiter != null)
      {
         rateLimiter.limit();
      }

      if (handler != null)
      {
         throw HornetQClientMessageBundle.BUNDLE.messageHandlerSet();
      }

      if (clientWindowSize == 0)
      {
         startSlowConsumer();
      }

      receiverThread = Thread.currentThread();

      // To verify if deliveryForced was already call
      boolean deliveryForced = false;
      // To control when to call deliveryForce
      boolean callForceDelivery = false;

      long start = -1;

      long toWait = timeout == 0 ? Long.MAX_VALUE : timeout;

      try
      {
         while (true)
         {
            ClientMessageInternal m = null;

            synchronized (this)
            {
               while ((stopped || (m = buffer.poll()) == null) && !closed && toWait > 0)
               {
                  if (start == -1)
                  {
                     start = System.currentTimeMillis();
                  }

                  if (m == null && forcingDelivery)
                  {
                     if (stopped)
                     {
                        break;
                     }

                     // we only force delivery once per call to receive
                     if (!deliveryForced)
                     {
                        callForceDelivery = true;
                        break;
                     }
                  }

                  try
                  {
                     wait(toWait);
                  }
                  catch (InterruptedException e)
                  {
                     throw new HornetQInterruptedException(e);
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

            if (failedOver)
            {
               if (m == null)
               {
                  // if failed over and the buffer is null, we reset the state and try it again
                  failedOver = false;
                  deliveryForced = false;
                  toWait = timeout == 0 ? Long.MAX_VALUE : timeout;
                  continue;
               }
               else
               {
                  failedOver = false;
               }
            }

            if (callForceDelivery)
            {
               if (isTrace)
               {
                  HornetQClientLogger.LOGGER.trace("Forcing delivery");
               }
               // JBPAPP-6030 - Calling forceDelivery outside of the lock to avoid distributed dead locks
               session.forceDelivery(id, forceDeliveryCount++);
               callForceDelivery = false;
               deliveryForced = true;
               continue;
            }

            if (m != null)
            {
               session.workDone();

               if (m.containsProperty(ClientConsumerImpl.FORCED_DELIVERY_MESSAGE))
               {
                  long seq = m.getLongProperty(ClientConsumerImpl.FORCED_DELIVERY_MESSAGE);

                  // Need to check if forceDelivery was called at this call
                  // As we could be receiving a message that came from a previous call
                  if (forcingDelivery && deliveryForced && seq == forceDeliveryCount - 1)
                  {
                     // forced delivery messages are discarded, nothing has been delivered by the queue
                     resetIfSlowConsumer();

                     if (isTrace)
                     {
                        HornetQClientLogger.LOGGER.trace("There was nothing on the queue, leaving it now:: returning null");
                     }

                     return null;
                  }
                  else
                  {
                     if (isTrace)
                     {
                        HornetQClientLogger.LOGGER.trace("Ignored force delivery answer as it belonged to another call");
                     }
                     // Ignore the message
                     continue;
                  }
               }
               // if we have already pre acked we cant expire
               boolean expired = m.isExpired();

               flowControlBeforeConsumption(m);

               if (expired)
               {
                  m.discardBody();

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
                  largeMessageReceived = m;
               }

               if (isTrace)
               {
                  HornetQClientLogger.LOGGER.trace("Returning " + m);
               }

               return m;
            }
            else
            {
               if (isTrace)
               {
                  HornetQClientLogger.LOGGER.trace("Returning null");
               }
               resetIfSlowConsumer();
               return null;
            }
         }
      }
      finally
      {
         receiverThread = null;
      }
   }

   public ClientMessage receive(final long timeout) throws HornetQException
   {
      ClientMessage msg = receive(timeout, false);

      if (msg == null && !closed)
      {
         msg = receive(0, true);
      }

      return msg;
   }

   public ClientMessage receive() throws HornetQException
   {
      return receive(0, false);
   }

   public ClientMessage receiveImmediate() throws HornetQException
   {
      return receive(0, true);
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
         throw HornetQClientMessageBundle.BUNDLE.inReceive();
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
         waitForOnMessageToComplete(true);
      }
   }

   public void close() throws HornetQException
   {
      doCleanUp(true);
   }

   /**
    * To be used by MDBs
    * @throws HornetQException
    */
   public void interruptHandlers() throws HornetQException
   {
      closing = true;

      resetLargeMessageController();

      Thread onThread = onMessageThread;
      if (onThread != null)
      {
         try
         {
            // just trying to interrupt any ongoing messages
            onThread.interrupt();
         }
         catch (Throwable ignored)
         {
            // security exception probably.. we just ignore it, not big deal!
         }
      }
   }

   public void cleanUp()
   {
      try
      {
         doCleanUp(false);
      }
      catch (HornetQException e)
      {
         HornetQClientLogger.LOGGER.warn("problem cleaning up: " + this);
      }
   }

   public boolean isClosed()
   {
      return closed;
   }

   public void stop(final boolean waitForOnMessage) throws HornetQException
   {
      waitForOnMessageToComplete(waitForOnMessage);

      if (browseOnly)
      {
         // stop shouldn't affect browser delivery
         return;
      }

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

      // failover will issue a start later
      this.stopped = true;

      resetLargeMessageController();

      lastAckedMessage = null;

      creditsToSend = 0;

      failedOver = true;

      ackIndividually = false;
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

   public SessionQueueQueryResponseMessage getQueueInfo()
   {
      return queueInfo;
   }

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

   public synchronized void handleMessage(final SessionReceiveMessage message) throws Exception
   {
      if (closing)
      {
         // This is ok - we just ignore the message
         return;
      }

      if (message.getMessage().getBooleanProperty(Message.HDR_LARGE_COMPRESSED))
      {
         handleCompressedMessage(message);
      }
      else
      {
         handleRegularMessage((ClientMessageInternal)message.getMessage(), message);
      }
   }

   private void handleRegularMessage(final ClientMessageInternal message, final SessionReceiveMessage messagePacket) throws Exception
   {
      message.setDeliveryCount(messagePacket.getDeliveryCount());

      message.setFlowControlSize(messagePacket.getPacketSize());

      handleRegularMessage(message);
   }

   private void handleRegularMessage(ClientMessageInternal message)
   {
      if (message.getAddress() == null)
      {
         message.setAddressTransient(queueInfo.getAddress());
      }

      message.onReceipt(this);

      if (message.getPriority() != 4)
      {
         // We have messages of different priorities so we need to ack them individually since the order
         // of them in the ServerConsumerImpl delivery list might not be the same as the order they are
         // consumed in, which means that acking all up to won't work
         ackIndividually = true;
      }

      // Add it to the buffer
      buffer.addTail(message, message.getPriority());

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

   /**
    * This method deals with messages arrived as regular message but its contents are compressed.
    * Such messages come from message senders who are configured to compress large messages, and
    * if some of the messages are compressed below the min-large-message-size limit, they are sent
    * as regular messages.
    *
    * However when decompressing the message, we are not sure how large the message could be..
    * for that reason we fake a large message controller that will deal with the message as it was a large message
    *
    * Say that you sent a 1G message full of spaces. That could be just bellow 100K compressed but you wouldn't have
    * enough memory to decompress it
    */
   private void handleCompressedMessage(final SessionReceiveMessage message) throws Exception
   {
      ClientMessageImpl clMessage = (ClientMessageImpl) message.getMessage();
      //create a ClientLargeMessageInternal out of the message
      ClientLargeMessageImpl largeMessage = new ClientLargeMessageImpl();
      largeMessage.retrieveExistingData(clMessage);

      File largeMessageCache = null;

      if (session.isCacheLargeMessageClient())
      {
         largeMessageCache = File.createTempFile("tmp-large-message-" + largeMessage.getMessageID() + "-",
                                                 ".tmp");
         largeMessageCache.deleteOnExit();
      }

      ClientSessionFactory sf = session.getSessionFactory();
      ServerLocator locator = sf.getServerLocator();
      long callTimeout = locator.getCallTimeout();

      currentLargeMessageController = new LargeMessageControllerImpl(this, largeMessage.getLargeMessageSize(), callTimeout, largeMessageCache);
      currentLargeMessageController.setLocal(true);

      //sets the packet
      HornetQBuffer qbuff = clMessage.getBodyBuffer();
      int bytesToRead = qbuff.writerIndex() - qbuff.readerIndex();
      final byte[] body = qbuff.readBytes(bytesToRead).toByteBuffer().array();

      largeMessage.setLargeMessageController(new CompressedLargeMessageControllerImpl(currentLargeMessageController));
      SessionReceiveContinuationMessage packet = new SessionReceiveContinuationMessage(this.getID(), body, false, false, body.length);
      currentLargeMessageController.addPacket(packet);

      handleRegularMessage(largeMessage, message);
   }

   public synchronized void handleLargeMessage(final SessionReceiveLargeMessage packet) throws Exception
   {
      if (closing)
      {
         // This is ok - we just ignore the message
         return;
      }

      // Flow control for the first packet, we will have others
      ClientLargeMessageInternal currentChunkMessage = (ClientLargeMessageInternal)packet.getLargeMessage();

      currentChunkMessage.setFlowControlSize(packet.getPacketSize());

      currentChunkMessage.setDeliveryCount(packet.getDeliveryCount());

      File largeMessageCache = null;

      if (session.isCacheLargeMessageClient())
      {
         largeMessageCache = File.createTempFile("tmp-large-message-" + currentChunkMessage.getMessageID() + "-",
                                                 ".tmp");
         largeMessageCache.deleteOnExit();
      }

      ClientSessionFactory sf = session.getSessionFactory();
      ServerLocator locator = sf.getServerLocator();
      long callTimeout = locator.getCallTimeout();

      currentLargeMessageController = new LargeMessageControllerImpl(this, packet.getLargeMessageSize(), callTimeout, largeMessageCache);

      if (currentChunkMessage.isCompressed())
      {
         currentChunkMessage.setLargeMessageController(new CompressedLargeMessageControllerImpl(currentLargeMessageController));
      }
      else
      {
         currentChunkMessage.setLargeMessageController(currentLargeMessageController);
      }

      handleRegularMessage(currentChunkMessage);
   }

   public synchronized void handleLargeMessageContinuation(final SessionReceiveContinuationMessage chunk) throws Exception
   {
      if (closing)
      {
         return;
      }
      if (currentLargeMessageController == null)
      {
         if (isTrace)
         {
            HornetQClientLogger.LOGGER.trace("Sending back credits for largeController = null " + chunk.getPacketSize());
         }
         flowControl(chunk.getPacketSize(), false);
      }
      else
      {
         currentLargeMessageController.addPacket(chunk);
      }
   }

   public void clear(boolean waitForOnMessage) throws HornetQException
   {
      synchronized (this)
      {
         // Need to send credits for the messages in the buffer

         Iterator<ClientMessageInternal> iter = buffer.iterator();

         while (iter.hasNext())
         {
            try
            {
               ClientMessageInternal message = iter.next();

               if (message.isLargeMessage())
               {
                  ClientLargeMessageInternal largeMessage = (ClientLargeMessageInternal)message;
                  largeMessage.getLargeMessageController().cancel();
               }

               flowControlBeforeConsumption(message);
            }
            catch (Exception e)
            {
               HornetQClientLogger.LOGGER.errorClearingMessages(e);
            }
         }

         clearBuffer();

         try
         {
            resetLargeMessageController();
         }
         catch (Throwable e)
         {
            // nothing that could be done here
            HornetQClientLogger.LOGGER.errorClearingMessages(e);
         }
      }

      // Need to send credits for the messages in the buffer

      waitForOnMessageToComplete(waitForOnMessage);
   }

   private void resetLargeMessageController()
   {

      LargeMessageController controller = currentLargeMessageController;
      if (controller != null)
      {
         controller.cancel();
         currentLargeMessageController = null;
      }
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
      ClientMessageInternal cmi = (ClientMessageInternal)message;

      if (ackIndividually)
      {
         individualAcknowledge(message);
      }
      else
      {
         ackBytes += message.getEncodeSize();

         if (ackBytes >= ackBatchSize)
         {
            doAck(cmi);
         }
         else
         {
            lastAckedMessage = cmi;
         }
      }
   }

   public void individualAcknowledge(ClientMessage message) throws HornetQException
   {
      if (lastAckedMessage != null)
      {
         flushAcks();
      }

      session.individualAcknowledge(id, message.getMessageID());
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
   * @param discountSlowConsumer When dealing with slowConsumers, we need to discount one credit that was pre-sent when the first receive was called. For largeMessage that is only done at the latest packet
   */
   public void flowControl(final int messageBytes, final boolean discountSlowConsumer) throws HornetQException
   {
      if (clientWindowSize >= 0)
      {
         creditsToSend += messageBytes;

         if (creditsToSend >= clientWindowSize)
         {
            if (clientWindowSize == 0 && discountSlowConsumer)
            {
               if (isTrace)
               {
                  HornetQClientLogger.LOGGER.trace("FlowControl::Sending " + creditsToSend + " -1, for slow consumer");
               }

               // sending the credits - 1 initially send to fire the slow consumer, or the slow consumer would be
               // always buffering one after received the first message
               final int credits = creditsToSend - 1;

               creditsToSend = 0;

               if (credits > 0)
               {
                  sendCredits(credits);
               }
            }
            else
            {
               if (HornetQClientLogger.LOGGER.isDebugEnabled())
               {
                  HornetQClientLogger.LOGGER.debug("Sending " + messageBytes + " from flow-control");
               }

               final int credits = creditsToSend;

               creditsToSend = 0;

               if (credits > 0)
               {
                  sendCredits(credits);
               }
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
      if (isTrace)
      {
         HornetQClientLogger.LOGGER.trace("Sending 1 credit to start delivering of one message to slow consumer");
      }
      sendCredits(1);
      try
      {
         // We use an executor here to guarantee the messages will arrive in order.
         // However when starting a slow consumer, we have to guarantee the credit was sent before we can perform any
         // operations like forceDelivery
         pendingFlowControl.await(10, TimeUnit.SECONDS);
      }
      catch (InterruptedException e)
      {
         // will just ignore and forward the ignored
         Thread.currentThread().interrupt();
      }
   }

   private void resetIfSlowConsumer()
   {
      if (clientWindowSize == 0)
      {
         sendCredits(0);

         // If resetting a slow consumer, we need to wait the execution
         final CountDownLatch latch = new CountDownLatch(1);
         flowControlExecutor.execute(new Runnable()
         {
            public void run()
            {
               latch.countDown();
            }
         });

         try
         {
            latch.await(10, TimeUnit.SECONDS);
         }
         catch (InterruptedException e)
         {
            throw new HornetQInterruptedException(e);
         }
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
      if (isTrace)
      {
         HornetQClientLogger.LOGGER.trace("Adding Runner on Executor for delivery");
      }

      sessionExecutor.execute(runner);
   }

   /**
    * @param credits
    */
   private void sendCredits(final int credits)
   {
      pendingFlowControl.countUp();
      flowControlExecutor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               channel.send(new SessionConsumerFlowCreditMessage(id, credits));
            }
            finally
            {
               pendingFlowControl.countDown();
            }
         }
      });
   }

   private void waitForOnMessageToComplete(boolean waitForOnMessage)
   {
      if (handler == null)
      {
         return;
      }

      if (!waitForOnMessage || Thread.currentThread() == onMessageThread)
      {
         // If called from inside onMessage then return immediately - otherwise would block
         return;
      }

      org.hornetq.utils.FutureLatch future = new FutureLatch();

      sessionExecutor.execute(future);

      boolean ok = future.await(ClientConsumerImpl.CLOSE_TIMEOUT_MILLISECONDS);

      if (!ok)
      {
         HornetQClientLogger.LOGGER.timeOutWaitingForProcessing();
      }
   }

   private void checkClosed() throws HornetQException
   {
      if (closed)
      {
         throw HornetQClientMessageBundle.BUNDLE.consumerClosed();
      }
   }

   private void callOnMessage() throws Exception
   {
      if (closing || stopped)
      {
         return;
      }

      session.workDone();

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

         failedOver = false;

         synchronized (this)
         {
            message = buffer.poll();
         }

         if (message != null)
         {
            if (message.containsProperty(ClientConsumerImpl.FORCED_DELIVERY_MESSAGE))
            {
               //Ignore, this could be a relic from a previous receiveImmediate();
               return;
            }



            boolean expired = message.isExpired();

            flowControlBeforeConsumption(message);

            if (!expired)
            {
               if (isTrace)
               {
                  HornetQClientLogger.LOGGER.trace("Calling handler.onMessage");
               }
               final ClassLoader originalLoader = AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
               {
                  public ClassLoader run()
                  {
                     ClassLoader originalLoader = Thread.currentThread().getContextClassLoader();

                     Thread.currentThread().setContextClassLoader(contextClassLoader);

                     return originalLoader;
                  }
               });

               onMessageThread = Thread.currentThread();
               try
               {
                  theHandler.onMessage(message);
               }
               finally
               {
                  try
                  {
                     AccessController.doPrivileged(new PrivilegedAction<Object>()
                     {
                        public Object run()
                        {
                           Thread.currentThread().setContextClassLoader(originalLoader);
                           return null;
                        }
                     });
                  }
                  catch (Exception e)
                  {
                     HornetQClientLogger.LOGGER.warn(e.getMessage(), e);
                  }

                  onMessageThread = null;
               }

               if (isTrace)
               {
                  HornetQClientLogger.LOGGER.trace("Handler.onMessage done");
               }

               if (message.isLargeMessage())
               {
                  message.discardBody();
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
         // on large messages we should discount 1 on the first packets as we need continuity until the last packet
         flowControl(message.getFlowControlSize(), !message.isLargeMessage());
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
         waitForOnMessageToComplete(true);

         resetLargeMessageController();

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
            channel.sendBlocking(new SessionConsumerCloseMessage(id), PacketImpl.NULL_RESPONSE);
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

   private void doAck(final ClientMessageInternal message) throws HornetQException
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
            HornetQClientLogger.LOGGER.onMessageError(e);

            lastException = e;
         }
      }
   }

}

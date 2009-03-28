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

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.ClientFileMessage;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerCloseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.Future;
import org.jboss.messaging.utils.TokenBucketLimiter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executor;

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

   // Attributes
   // -----------------------------------------------------------------------------------

   private final ClientSessionInternal session;

   private final Channel channel;

   private final long id;

   private final Executor sessionExecutor;

   private final int clientWindowSize;

   private final int ackBatchSize;

   private final Queue<ClientMessageInternal> buffer = new LinkedList<ClientMessageInternal>();

   private final Runner runner = new Runner();

   private final File directory;

   private ClientMessageInternal currentChunkMessage;
   
   private final TokenBucketLimiter rateLimiter;

   private volatile Thread receiverThread;

   private volatile Thread onMessageThread;

   private volatile MessageHandler handler;

   private volatile boolean closing;

   private volatile boolean closed;

   private volatile int creditsToSend;

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
      
      if (rateLimiter != null)
      {
         rateLimiter.limit();
      }


      if (handler != null)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE,
                                      "Cannot call receive(...) - a MessageHandler is set");
      }

      if (clientWindowSize == 0 && buffer.isEmpty())
      {
         sendCredits(1);
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
               while ((stopped || (m = buffer.poll()) == null) &&
                      !closed && toWait > 0)
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
                  session.expire(id, m.getMessageID());

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
         sendCredits(1);
      }

      handler = theHandler;

      //if no previous handler existed queue up messages for delivery
      if (handler != null && noPreviousHandler)
      {
         for (int i = 0; i < buffer.size(); i++)
         {
            queueExecutor();
         }
      }
      //if unsetting a previous handler may be in onMessage so wait for completion
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
      for (int i = 0; i < buffer.size(); i++)
      {
         queueExecutor();
      }
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

      if (isFileConsumer())
      {
         messageToHandle = cloneAsFileMessage(message);
      }

      messageToHandle.onReceipt(this);

      if (handler != null)
      {
         // Execute using executor

         buffer.add(messageToHandle);
         if (!stopped)
         {
            queueExecutor();
         }
      }
      else
      {
         // Add it to the buffer
         buffer.add(messageToHandle);

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
      flowControl(packet.getPacketSize(), true);

      currentChunkMessage = createFileMessage(packet.getLargeMessageHeader());
   }

   public synchronized void handleLargeMessageContinuation(final SessionReceiveContinuationMessage chunk) throws Exception
   {
      if (closing)
      {
         return;
      }

      if (chunk.isContinues())
      {
         flowControl(chunk.getPacketSize(), true);
      }

      if (isFileConsumer())
      {
         ClientFileMessageInternal fileMessage = (ClientFileMessageInternal) currentChunkMessage;
         addBytesBody(fileMessage, chunk.getBody());
      }
      else
      {
         if (currentChunkMessage.getBody() == null)
         {
            currentChunkMessage.setBody(ChannelBuffers.dynamicBuffer(chunk.getBody()));
         }
         else
         {
            currentChunkMessage.getBody().writeBytes(chunk.getBody());
         }
      }

      if (!chunk.isContinues())
      {
         // Close the file that was being generated
         if (isFileConsumer())
         {
            ((ClientFileMessageInternal) currentChunkMessage).closeChannel();
         }

         currentChunkMessage.setFlowControlSize(chunk.getPacketSize());

         ClientMessageInternal msgToSend = currentChunkMessage;

         currentChunkMessage = null;

         handleMessage(msgToSend);
      }
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

   public int getCreditsToSend()
   {
      return creditsToSend;
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

   // Public7
   // ---------------------------------------------------------------------------------------

   // Package protected
   // ---------------------------------------------------------------------------------------

   // Protected
   // ---------------------------------------------------------------------------------------

   // Private
   // ---------------------------------------------------------------------------------------

   private void queueExecutor()
   {
      sessionExecutor.execute(runner);
   }

   private void flowControl(final int messageBytes, final boolean isLargeMessage) throws MessagingException
   {
      if (clientWindowSize >= 0)
      {
         creditsToSend += messageBytes;

         if (creditsToSend >= clientWindowSize)
         {

            if (isLargeMessage)
            {
               // Flowcontrol on largeMessages continuations needs to be done in a separate thread or failover would block
               final int credits = creditsToSend;

               creditsToSend = 0;
               sessionExecutor.execute(new Runnable()
               {
                  public void run()
                  {
                     sendCredits(credits);
                  }


               });
            }
            else
            {
               if (clientWindowSize == 0)
               {
                  // sending the credits - 1 initially send to fire the slow consumer, or the slow consumer would be aways buffering one after received the first message
                  sendCredits(creditsToSend - 1);
               }
               else
               {
                  sendCredits(creditsToSend);
               }
               creditsToSend = 0;
            }
         }
      }
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
            message = buffer.poll();
         }

         if (message != null)
         {
            boolean expired = message.isExpired();

            flowControlBeforeConsumption(message);

            if (!expired)
            {
               onMessageThread = Thread.currentThread();

               theHandler.onMessage(message);
            }
            else
            {
               session.expire(id, message.getMessageID());
            }
            
            if (clientWindowSize == 0)
            {
               sendCredits(1);
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
      flowControl(message.getFlowControlSize(), false);
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
      if (isFileConsumer())
      {
         for (ClientMessage message : buffer)
         {
            if (message instanceof ClientFileMessage)
            {
               ((ClientFileMessage) message).getFile().delete();
            }
         }
      }
      buffer.clear();
   }

   private void doAck(final ClientMessage message) throws MessagingException
   {
      ackBytes = 0;

      lastAckedMessage = null;

      session.acknowledge(id, message.getMessageID());
   }

   private ClientMessageInternal cloneAsFileMessage(final ClientMessageInternal message) throws Exception
   {
      if (message instanceof ClientFileMessageImpl)
      {
         // nothing to be done
         return message;
      }
      else
      {
         int propertiesSize = message.getPropertiesEncodeSize();

         MessagingBuffer bufferProperties = session.createBuffer(propertiesSize);

         // FIXME: Find a better way to clone this ClientMessageImpl as ClientFileMessageImpl without using the
         // MessagingBuffer.
         // There is no direct access into the Properties, and I couldn't add a direct cast to this method without loose
         // abstraction
         message.encodeProperties(bufferProperties);

         bufferProperties.resetReaderIndex();

         ClientFileMessageImpl cloneMessage = new ClientFileMessageImpl();

         cloneMessage.decodeProperties(bufferProperties);

         cloneMessage.setDeliveryCount(message.getDeliveryCount());

         cloneMessage.setLargeMessage(message.isLargeMessage());

         cloneMessage.setFile(new File(directory, cloneMessage.getMessageID() + "-" +
                                                  session.getName() +
                                                  "-" +
                                                  getID() +
                                                  ".jbm"));

         cloneMessage.setFlowControlSize(message.getFlowControlSize());

         addBytesBody(cloneMessage, message.getBody().array());

         cloneMessage.closeChannel();

         return cloneMessage;
      }
   }

   private ClientMessageInternal createFileMessage(final byte[] header) throws Exception
   {

      MessagingBuffer headerBuffer = ChannelBuffers.wrappedBuffer(header);

      if (isFileConsumer())
      {
         if (!directory.exists())
         {
            boolean ok = directory.mkdirs();

            if (!ok)
            {
               throw new IOException("Failed to create directory " + directory.getCanonicalPath());
            }
         }

         ClientFileMessageImpl message = new ClientFileMessageImpl();
         message.decodeProperties(headerBuffer);
         message.setFile(new File(directory, message.getMessageID() + "-" + session.getName() + "-" + getID() + ".jbm"));
         message.setLargeMessage(true);
         return message;
      }
      else
      {
         ClientMessageImpl message = new ClientMessageImpl();
         message.decodeProperties(headerBuffer);
         message.setLargeMessage(true);
         return message;
      }
   }

   private void addBytesBody(final ClientFileMessageInternal fileMessage, final byte[] body) throws Exception
   {
      FileChannel channel = fileMessage.getChannel();
      channel.write(ByteBuffer.wrap(body));
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

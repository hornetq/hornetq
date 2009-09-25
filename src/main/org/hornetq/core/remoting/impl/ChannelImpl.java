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

package org.hornetq.core.remoting.impl;

import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.PACKETS_CONFIRMED;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.CommandConfirmationHandler;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.PacketsConfirmedMessage;
import org.hornetq.core.remoting.spi.HornetQBuffer;

/**
 * A ChannelImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ChannelImpl implements Channel
{
   private static final Logger log = Logger.getLogger(ChannelImpl.class);

   private volatile long id;

   private ChannelHandler handler;

   private Packet response;

   private final java.util.Queue<Packet> resendCache;

   private volatile int firstStoredCommandID;

   private volatile int lastReceivedCommandID = -1;

   private volatile RemotingConnection connection;

   private volatile boolean closed;

   private final Lock lock = new ReentrantLock();

   private final Condition sendCondition = lock.newCondition();

   private final Condition failoverCondition = lock.newCondition();

   private final Object sendLock = new Object();

   private final Object sendBlockingLock = new Object();

   private boolean failingOver;

   private final int windowSize;

   private final int confWindowSize;

   private final Semaphore sendSemaphore;

   private int receivedBytes;

   private CommandConfirmationHandler commandConfirmationHandler;

   public ChannelImpl(final RemotingConnection connection, final long id, final int windowSize, final boolean block)
   {
      this.connection = connection;

      this.id = id;

      this.windowSize = windowSize;

      this.confWindowSize = (int)(0.75 * windowSize);

      if (this.windowSize != -1)
      {
         resendCache = new ConcurrentLinkedQueue<Packet>();

         if (block)
         {
            sendSemaphore = new Semaphore(windowSize, true);
         }
         else
         {
            sendSemaphore = null;
         }
      }
      else
      {
         resendCache = null;

         sendSemaphore = null;
      }
   }

   public long getID()
   {
      return id;
   }

   public int getLastReceivedCommandID()
   {
      return lastReceivedCommandID;
   }

   public Lock getLock()
   {
      return lock;
   }

   public void returnBlocking()
   {
      lock.lock();

      try
      {
         response = new HornetQExceptionMessage(new HornetQException(HornetQException.UNBLOCKED,
                                                                     "Connection failure detected. Unblocking a blocking call that will never get a response"

         ));

         sendCondition.signal();
      }
      finally
      {
         lock.unlock();
      }
   }

   public void sendAndFlush(final Packet packet)
   {
      send(packet, true);
   }

   public void send(final Packet packet)
   {
      send(packet, false);
   }

   // This must never called by more than one thread concurrently
   public void send(final Packet packet, final boolean flush)
   {
      synchronized (sendLock)
      {
         packet.setChannelID(id);

         final HornetQBuffer buffer = connection.getTransportConnection().createBuffer(packet.getRequiredBufferSize());

         int size = packet.encode(buffer);

         lock.lock();

         try
         {
            while (failingOver)
            {
               // TODO - don't hardcode this timeout
               try
               {
                  failoverCondition.await(10000, TimeUnit.MILLISECONDS);
               }
               catch (InterruptedException e)
               {
               }
            }

            if (resendCache != null && packet.isRequiresConfirmations())
            {
               resendCache.add(packet);
            }

            connection.getTransportConnection().write(buffer, flush);
         }
         finally
         {
            lock.unlock();
         }
         
         // Must block on semaphore outside the main lock or this can prevent failover from occurring, also after the
         // packet is sent to assure we get some credits back
         if (sendSemaphore != null && packet.getType() != PACKETS_CONFIRMED)
         {
            try
            {
               sendSemaphore.acquire(size);
            }
            catch (InterruptedException e)
            {
               throw new IllegalStateException("Semaphore interrupted");
            }
         }
      }
   }

   public Packet sendBlocking(final Packet packet) throws HornetQException
   {
      if (closed)
      {
         throw new HornetQException(HornetQException.NOT_CONNECTED, "Connection is destroyed");
      }

      if (connection.getBlockingCallTimeout() == -1)
      {
         throw new IllegalStateException("Cannot do a blocking call timeout on a server side connection");
      }

      // Synchronized since can't be called concurrently by more than one thread and this can occur
      // E.g. blocking acknowledge() from inside a message handler at some time as other operation on main thread
      synchronized (sendBlockingLock)
      {
         packet.setChannelID(id);

         final HornetQBuffer buffer = connection.getTransportConnection().createBuffer(packet.getRequiredBufferSize());

         int size = packet.encode(buffer);

         lock.lock();

         try
         {
            while (failingOver)
            {
               // TODO - don't hardcode this timeout
               try
               {
                  failoverCondition.await(10000, TimeUnit.MILLISECONDS);
               }
               catch (InterruptedException e)
               {
               }
            }

            response = null;

            if (resendCache != null && packet.isRequiresConfirmations())
            {
               resendCache.add(packet);
            }

            connection.getTransportConnection().write(buffer);

            long toWait = connection.getBlockingCallTimeout();

            long start = System.currentTimeMillis();

            while (response == null && toWait > 0)
            {
               try
               {
                  sendCondition.await(toWait, TimeUnit.MILLISECONDS);
               }
               catch (InterruptedException e)
               {
               }

               if (closed)
               {
                  break;
               }

               final long now = System.currentTimeMillis();

               toWait -= now - start;

               start = now;
            }

            if (response == null)
            {
               throw new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                          "Timed out waiting for response when sending packet " + packet.getType());
            }

            if (response.getType() == PacketImpl.EXCEPTION)
            {
               final HornetQExceptionMessage mem = (HornetQExceptionMessage)response;
               
               HornetQException e = mem.getException();
               
               e.fillInStackTrace();

               throw e;
            }
         }
         finally
         {
            lock.unlock();
         }
         // Must block on semaphore outside the main lock or this can prevent failover from occurring, also after the
         // packet is sent to assure we get some credits back
         if (sendSemaphore != null && packet.getType() != PACKETS_CONFIRMED)
         {
            try
            {
               sendSemaphore.acquire(size);
            }
            catch (InterruptedException e)
            {
               throw new IllegalStateException("Semaphore interrupted");
            }
         }
         return response;
      }
   }

   public void setCommandConfirmationHandler(final CommandConfirmationHandler handler)
   {
      this.commandConfirmationHandler = handler;
   }

   public void setHandler(final ChannelHandler handler)
   {
      this.handler = handler;
   }

   public ChannelHandler getHandler()
   {
      return handler;
   }

   public void close()
   {
      if (closed)
      {
         return;
      }

      if (!connection.isDestroyed() && !connection.removeChannel(id))
      {
         throw new IllegalArgumentException("Cannot find channel with id " + id + " to close");
      }

      closed = true;
   }

   public void transferConnection(final RemotingConnection newConnection)
   {
      // Needs to synchronize on the connection to make sure no packets from
      // the old connection get processed after transfer has occurred
      synchronized (connection.getTransferLock())
      {
         connection.removeChannel(id);

         // And switch it

         final RemotingConnectionImpl rnewConnection = (RemotingConnectionImpl)newConnection;

         rnewConnection.putChannel(id, this);

         connection = rnewConnection;
      }
   }

   public void replayCommands(final int otherLastReceivedCommandID, final long newChannelID)
   {
      clearUpTo(otherLastReceivedCommandID);

      for (final Packet packet : resendCache)
      {
         packet.setChannelID(newChannelID);

         doWrite(packet);
      }
   }

   public void lock()
   {
      lock.lock();

      failingOver = true;

      lock.unlock();
   }

   public void unlock()
   {
      lock.lock();

      failingOver = false;

      failoverCondition.signalAll();

      lock.unlock();
   }

   public RemotingConnection getConnection()
   {
      return connection;
   }

   public void flushConfirmations()
   {
      if (receivedBytes != 0)
      {
         receivedBytes = 0;

         final Packet confirmed = new PacketsConfirmedMessage(lastReceivedCommandID);

         confirmed.setChannelID(id);

         doWrite(confirmed);
      }
   }

   public void confirm(final Packet packet)
   {
      if (resendCache != null && packet.isRequiresConfirmations())
      {
         lastReceivedCommandID++;

         receivedBytes += packet.getPacketSize();

         if (receivedBytes >= confWindowSize)
         {
            receivedBytes = 0;

            final Packet confirmed = new PacketsConfirmedMessage(lastReceivedCommandID);

            confirmed.setChannelID(id);

            doWrite(confirmed);
         }
      }
   }

   public void handlePacket(final Packet packet)
   {
      if (packet.getType() == PACKETS_CONFIRMED)
      {
         if (resendCache != null)
         {
            final PacketsConfirmedMessage msg = (PacketsConfirmedMessage)packet;

            clearUpTo(msg.getCommandID());
         }

         if (!connection.isClient())
         {
            handler.handlePacket(packet);
         }

         return;
      }
      else
      {
         if (packet.isResponse())
         {
            confirm(packet);

            lock.lock();

            response = packet;

            try
            {
               sendCondition.signal();
            }
            finally
            {
               lock.unlock();
            }
         }
         else if (handler != null)
         {
            handler.handlePacket(packet);
         }
      }
   }

   private void doWrite(final Packet packet)
   {
      final HornetQBuffer buffer = connection.getTransportConnection().createBuffer(packet.getRequiredBufferSize());

      packet.encode(buffer);

      connection.getTransportConnection().write(buffer);
   }

   private void clearUpTo(final int lastReceivedCommandID)
   {
      final int numberToClear = 1 + lastReceivedCommandID - firstStoredCommandID;

      if (numberToClear == -1)
      {
         throw new IllegalArgumentException("Invalid lastReceivedCommandID: " + lastReceivedCommandID);
      }

      int sizeToFree = 0;

      for (int i = 0; i < numberToClear; i++)
      {
         final Packet packet = resendCache.poll();

         if (packet == null)
         {
            throw new IllegalStateException(System.identityHashCode(this) + " Can't find packet to clear: " +
                                            " last received command id " +
                                            lastReceivedCommandID +
                                            " first stored command id " +
                                            firstStoredCommandID);
         }

         if (packet.getType() != PACKETS_CONFIRMED)
         {
            sizeToFree += packet.getPacketSize();
         }

         if (commandConfirmationHandler != null)
         {
            commandConfirmationHandler.commandConfirmed(packet);
         }
      }

      firstStoredCommandID += numberToClear;

      if (sendSemaphore != null)
      {
         sendSemaphore.release(sizeToFree);
      }
   }
}

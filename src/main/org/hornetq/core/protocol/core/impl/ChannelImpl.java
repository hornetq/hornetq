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

package org.hornetq.core.protocol.core.impl;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CommandConfirmationHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.PacketsConfirmedMessage;

/**
 * A ChannelImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ChannelImpl implements Channel
{
   private static final Logger log = Logger.getLogger(ChannelImpl.class);

   private static final boolean isTrace = log.isTraceEnabled();

   private volatile long id;

   private ChannelHandler handler;

   private Packet response;

   private final java.util.Queue<Packet> resendCache;

   private volatile int firstStoredCommandID;

   private volatile int lastConfirmedCommandID = -1;

   private volatile CoreRemotingConnection connection;

   private volatile boolean closed;

   private final Lock lock = new ReentrantLock();

   private final Condition sendCondition = lock.newCondition();

   private final Condition failoverCondition = lock.newCondition();

   private final Object sendLock = new Object();

   private final Object sendBlockingLock = new Object();

   private boolean failingOver;

   private final int confWindowSize;

   private int receivedBytes;

   private CommandConfirmationHandler commandConfirmationHandler;

   private volatile boolean transferring;

   public ChannelImpl(final CoreRemotingConnection connection, final long id, final int confWindowSize)
   {
      this.connection = connection;

      this.id = id;

      this.confWindowSize = confWindowSize;

      if (confWindowSize != -1)
      {
         resendCache = new ConcurrentLinkedQueue<Packet>();
      }
      else
      {
         resendCache = null;
      }
   }

   public boolean supports(final byte packetType)
   {
      int version = connection.getClientVersion();

      switch (packetType)
      {
         case PacketImpl.CLUSTER_TOPOLOGY_V2:
            return version >= 122;
         default:
            return true;
      }
   }

   public long getID()
   {
      return id;
   }

   public int getLastConfirmedCommandID()
   {
      return lastConfirmedCommandID;
   }

   public Lock getLock()
   {
      return lock;
   }

   public int getConfirmationWindowSize()
   {
      return confWindowSize;
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
      send(packet, true, false);
   }

   public void send(final Packet packet)
   {
      send(packet, false, false);
   }

   public void sendBatched(final Packet packet)
   {
      send(packet, false, true);
   }

   public void setTransferring(boolean transferring)
   {
      this.transferring = transferring;
   }

   // This must never called by more than one thread concurrently

   public void send(final Packet packet, final boolean flush, final boolean batch)
   {
      synchronized (sendLock)
      {
         packet.setChannelID(id);

         if (isTrace)
         {
            log.trace("Sending packet nonblocking " + packet + " on channeID=" + id);
         }

         HornetQBuffer buffer = packet.encode(connection);

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
                  throw new HornetQInterruptedException(e);
               }
            }

            // Sanity check
            if (transferring)
            {
               throw new IllegalStateException("Cannot send a packet while channel is doing failover");
            }

            if (resendCache != null && packet.isRequiresConfirmations())
            {
               resendCache.add(packet);
            }
         }
         finally
         {
            lock.unlock();
         }

         if (isTrace)
         {
            log.trace("Writing buffer for channelID=" + id);
         }


         // The actual send must be outside the lock, or with OIO transport, the write can block if the tcp
         // buffer is full, preventing any incoming buffers being handled and blocking failover
         connection.getTransportConnection().write(buffer, flush, batch);
      }
   }

   /**
    * Due to networking issues or server issues the server may take longer to answer than expected.. the client may timeout the call throwing an exception
    * and the client could eventually retry another call, but the server could then answer a previous command issuing a class-cast-exception.
    * The expectedPacket will be used to filter out undesirable packets that would belong to previous calls.
    */
   public Packet sendBlocking(final Packet packet, byte expectedPacket) throws HornetQException
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

         final HornetQBuffer buffer = packet.encode(connection);

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
                  throw new HornetQInterruptedException(e);
               }
            }

            response = null;

            if (resendCache != null && packet.isRequiresConfirmations())
            {
               resendCache.add(packet);
            }

            connection.getTransportConnection().write(buffer, false, false);

            long toWait = connection.getBlockingCallTimeout();

            long start = System.currentTimeMillis();

            while (!closed && (response == null || (response.getType() != PacketImpl.EXCEPTION &&
                     response.getType() != expectedPacket)) && toWait > 0)
            {
               try
               {
                  sendCondition.await(toWait, TimeUnit.MILLISECONDS);
               }
               catch (InterruptedException e)
               {
                  throw new HornetQInterruptedException(e);
               }
               
               if (response != null && response.getType() != PacketImpl.EXCEPTION && response.getType() != expectedPacket)
               {
                  log.warn("Packet " + response + " was answered out of sequence due to a previous server timeout and it's being ignored", new Exception ("trace"));
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

         return response;
      }
   }

   public void setCommandConfirmationHandler(final CommandConfirmationHandler handler)
   {
      if (confWindowSize < 0)
      {
         throw new IllegalStateException("You can't set confirmationHandler on a connection with confirmation-window-size < 0. Look at the documentation for more information.");
      }
      commandConfirmationHandler = handler;
   }

   public void setHandler(final ChannelHandler handler)
   {
      this.handler = handler;
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

      if(failingOver)
      {
         unlock();
      }
      closed = true;
   }

   public void transferConnection(final CoreRemotingConnection newConnection)
   {
      // Needs to synchronize on the connection to make sure no packets from
      // the old connection get processed after transfer has occurred
      synchronized (connection.getTransferLock())
      {
         connection.removeChannel(id);

         // And switch it

         final CoreRemotingConnection rnewConnection = (CoreRemotingConnection)newConnection;

         rnewConnection.putChannel(id, this);

         connection = rnewConnection;

         transferring = true;
      }
   }

   public void replayCommands(final int otherLastConfirmedCommandID)
   {
      if (resendCache != null)
      {
         if (isTrace)
         {
            log.trace("Replaying commands on channelID=" + id);
         }
         clearUpTo(otherLastConfirmedCommandID);

         for (final Packet packet : resendCache)
         {
            doWrite(packet);
         }
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

   public CoreRemotingConnection getConnection()
   {
      return connection;
   }

   // Needs to be synchronized since can be called by remoting service timer thread too for timeout flush
   public synchronized void flushConfirmations()
   {
      if (resendCache != null && receivedBytes != 0)
      {
         receivedBytes = 0;

         final Packet confirmed = new PacketsConfirmedMessage(lastConfirmedCommandID);

         confirmed.setChannelID(id);

         doWrite(confirmed);
      }
   }

   public void confirm(final Packet packet)
   {
      if (resendCache != null && packet.isRequiresConfirmations())
      {
         lastConfirmedCommandID++;

         receivedBytes += packet.getPacketSize();

         if (receivedBytes >= confWindowSize)
         {
            receivedBytes = 0;

            final Packet confirmed = new PacketsConfirmedMessage(lastConfirmedCommandID);

            confirmed.setChannelID(id);

            doWrite(confirmed);
         }
      }
   }

   public void clearCommands()
   {
      if (resendCache != null)
      {
         lastConfirmedCommandID = -1;

         firstStoredCommandID = 0;

         resendCache.clear();
      }
   }

   public void handlePacket(final Packet packet)
   {
      if (packet.getType() == PacketImpl.PACKETS_CONFIRMED)
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
      final HornetQBuffer buffer = packet.encode(connection);

      connection.getTransportConnection().write(buffer, false, false);
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
            ChannelImpl.log.warn("Can't find packet to clear: " + " last received command id " +
                                 lastReceivedCommandID +
                                 " first stored command id " +
                                 firstStoredCommandID);
            firstStoredCommandID = lastReceivedCommandID + 1;
            return;
         }

         if (packet.getType() != PacketImpl.PACKETS_CONFIRMED)
         {
            sizeToFree += packet.getPacketSize();
         }

         if (commandConfirmationHandler != null)
         {
            commandConfirmationHandler.commandConfirmed(packet);
         }
      }

      firstStoredCommandID += numberToClear;
   }
}

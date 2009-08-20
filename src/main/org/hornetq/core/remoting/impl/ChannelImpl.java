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

import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.EARLY_RESPONSE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.PACKETS_CONFIRMED;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.REPLICATION_RESPONSE;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

   private final Object replicationLock = new Object();

   private boolean failingOver;

   private final Queue<Runnable> responseActions = new ConcurrentLinkedQueue<Runnable>();

   private final int windowSize;

   private final int confWindowSize;

   private final Semaphore sendSemaphore;

   private int receivedBytes;

   private CommandConfirmationHandler commandConfirmationHandler;

   private int responseActionCount;

   private boolean playedResponsesOnFailure;

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
         response = new PacketImpl(EARLY_RESPONSE);

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

         final HornetQBuffer buffer = connection.getTransportConnection()
                                                  .createBuffer(packet.getRequiredBufferSize());

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

            if (connection.isActive() || packet.isWriteAlways())
            {
               connection.getTransportConnection().write(buffer, flush);
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

         final HornetQBuffer buffer = connection.getTransportConnection()
                                                  .createBuffer(packet.getRequiredBufferSize());

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

               throw mem.getException();
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

   // Must be synchronized since can be called by incoming session commands but also by deliveries
   // Also needs to be synchronized with respect to replicatingChannelDead
   public void replicatePacket(final Packet packet, final long replicatedChannelID, final Runnable action)
   {
      packet.setChannelID(replicatedChannelID);

      boolean runItNow = false;

      synchronized (replicationLock)
      {
         if (playedResponsesOnFailure && action != null)
         {
            // Already replicating channel failed, so just play the action now

            runItNow = true;
         }
         else
         {
            if (action != null)
            {
               responseActions.add(action);

               responseActionCount++;
            }

            final HornetQBuffer buffer = connection.getTransportConnection()
                                                     .createBuffer(packet.getRequiredBufferSize());

            packet.encode(buffer);

            connection.getTransportConnection().write(buffer);
         }
      }

      // Execute outside lock

      if (runItNow)
      {
         action.run();
      }
   }

   public void setCommandConfirmationHandler(final CommandConfirmationHandler handler)
   {
      this.commandConfirmationHandler = handler;
   }

   public void executeOutstandingDelayedResults()
   {
      // Execute on different thread to avoid deadlock

      new Thread()
      {
         public void run()
         {
            doExecuteOutstandingDelayedResults();
         }
      }.start();
   }

   private void doExecuteOutstandingDelayedResults()
   {
      List<Runnable> toRun = new ArrayList<Runnable>();

      synchronized (replicationLock)
      {         
         playedResponsesOnFailure = true;
         
         responseActionCount = 0;
      }

      while (true)
      {
         // Execute all the response actions now
         
         Runnable action = responseActions.poll();

         if (action != null)
         {
            toRun.add(action);
         }
         else
         {
            break;
         }
      }
      
      for (Runnable action : toRun)
      {
         action.run();
      }      
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

   public void transferConnection(final RemotingConnection newConnection,
                                  final long newChannelID,
                                  final Channel replicatingChannel)
   {
      // Needs to synchronize on the connection to make sure no packets from
      // the old connection get processed after transfer has occurred
      synchronized (connection.getTransferLock())
      {
         connection.removeChannel(id);

         if (replicatingChannel != null)
         {
            // If we're reconnecting to a live node which is replicated then there will be a replicating channel
            // too. We need to then make sure that all replication responses come back since packets aren't
            // considered confirmed until response comes back and is processed. Otherwise responses to previous
            // message sends could come back after reconnection resulting in clients resending same message
            // since it wasn't confirmed yet.
            replicatingChannel.waitForAllReplicationResponse();
         }

         // And switch it

         final RemotingConnectionImpl rnewConnection = (RemotingConnectionImpl)newConnection;

         rnewConnection.putChannel(newChannelID, this);

         connection = rnewConnection;

         this.id = newChannelID;
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
      if (receivedBytes != 0 && connection.isActive())
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

            if (connection.isActive())
            {
               final Packet confirmed = new PacketsConfirmedMessage(lastReceivedCommandID);

               confirmed.setChannelID(id);

               doWrite(confirmed);
            }
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
      else if (packet.getType() == REPLICATION_RESPONSE)
      {
         replicateResponseReceived();

         return;
      }
      else
      {
         if (packet.isResponse())
         {
            response = packet;

            confirm(packet);

            lock.lock();

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

      replicateComplete();
   }

   public void waitForAllReplicationResponse()
   {
      synchronized (replicationLock)
      {
         long toWait = 10000; // TODO don't hardcode timeout

         long start = System.currentTimeMillis();

         while (responseActionCount > 0 && toWait > 0)
         {
            try
            {
               replicationLock.wait();
            }
            catch (InterruptedException e)
            {
            }

            long now = System.currentTimeMillis();

            toWait -= now - start;

            start = now;
         }

         if (toWait <= 0)
         {
            log.warn("Timed out waiting for replication responses to return");
         }
      }
   }

   private void replicateComplete()
   {
      if (!connection.isActive() && id != 0)
      {
         // We're on backup and not ping channel so send back a replication response

         Packet packet = new PacketImpl(REPLICATION_RESPONSE);

         packet.setChannelID(2);

         doWrite(packet);
      }
   }

   // This will never get called concurrently by more than one thread

   // TODO it's not ideal synchronizing this since it forms a contention point with replication
   // but we need to do this to protect it w.r.t. the check on replicatingChannel

   private void replicateResponseReceived()
   {
      Runnable result = null;

      synchronized (replicationLock)
      {
         if (playedResponsesOnFailure)
         {
            return;
         }

         result = responseActions.poll();

         if (result == null)
         {
            throw new IllegalStateException("Cannot find response action");
         }
      }

      // Must execute outside of lock
      if (result != null)
      {
         result.run();

         // TODO - we can optimise this not to lock every time - only if waiting for all replications to return
         synchronized (replicationLock)
         {
            responseActionCount--;

            if (responseActionCount == 0)
            {
               replicationLock.notify();
            }
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

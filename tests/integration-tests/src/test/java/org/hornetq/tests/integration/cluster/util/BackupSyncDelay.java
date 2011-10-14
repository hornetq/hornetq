/**
 *
 */
package org.hornetq.tests.integration.cluster.util;

import java.util.concurrent.locks.Lock;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CommandConfirmationHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * An interceptor to keep a replicated backup server from reaching "up-to-date" status.
 * <p>
 * One problem is that we can't add an interceptor to the backup before starting it. So we add the
 * interceptor to the 'live' which will place a different {@link ChannelHandler} in the backup
 * during the initialization of replication.
 * <p>
 * We need to hijack the replication channel handler, because we need to
 * <ol>
 * <li>send an early answer to the {@link PacketImpl#REPLICATION_SYNC_FILE} packet that signals being
 * up-to-date
 * <li>not send an answer to it, when we deliver the packet later.
 * </ol>
 */
public class BackupSyncDelay implements Interceptor
{

   private final ReplicationChannelHandler handler = new ReplicationChannelHandler();
   private final TestableServer backup;
   private final TestableServer live;

   public void deliverUpToDateMsg()
   {
      live.removeInterceptor(this);
      if (backup.isStarted())
         handler.deliver();
   }

   public BackupSyncDelay(TestableServer backup, TestableServer live)
   {
      assert backup.getServer().getConfiguration().isBackup();
      assert !live.getServer().getConfiguration().isBackup();
      this.backup = backup;
      this.live = live;
      live.addInterceptor(this);
   }

   @Override
   public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
   {
      if (packet.getType() == PacketImpl.BACKUP_REGISTRATION)
      {
         try
         {
            ReplicationEndpoint repEnd = backup.getServer().getReplicationEndpoint();
            handler.addSubHandler(repEnd);
            Channel repChannel = repEnd.getChannel();
            repChannel.setHandler(handler);
            handler.setChannel(repChannel);
            live.removeInterceptor(this);
         }
         catch (Exception e)
         {
            throw new RuntimeException(e);
         }
      }
      return true;
   }

   public static class ReplicationChannelHandler implements ChannelHandler
   {

      private ReplicationEndpoint handler;
      private Packet onHold;
      private Channel channel;
      public volatile boolean deliver;
      private volatile boolean delivered;
      private boolean receivedUpToDate;
      private boolean mustHold = true;

      public void addSubHandler(ReplicationEndpoint handler)
      {
         this.handler = handler;
      }

      public synchronized void deliver()
      {
         deliver = true;
         if (!receivedUpToDate)
            return;
         if (delivered)
            return;

         if (onHold == null)
         {
            throw new NullPointerException("Don't have the 'sync is done' packet to deliver");
         }
         // Use wrapper to avoid sending a response
         ChannelWrapper wrapper = new ChannelWrapper(channel);
         handler.setChannel(wrapper);
         try
         {
            handler.handlePacket(onHold);
            delivered = true;
         }
         finally
         {
            handler.setChannel(channel);
            channel.setHandler(handler);
            onHold = null;
         }
      }

      public void setChannel(Channel channel)
      {
         this.channel = channel;
      }

      public void setHold(boolean hold)
      {
         mustHold = hold;
      }

      @Override
      public synchronized void handlePacket(Packet packet)
      {

         if (onHold != null && deliver)
         {
            deliver();
         }

         if (packet.getType() == PacketImpl.REPLICATION_START_STOP_SYNC && mustHold)
         {
            ReplicationStartSyncMessage syncMsg = (ReplicationStartSyncMessage)packet;
            if (syncMsg.isSynchronizationFinished() && !deliver)
            {
               receivedUpToDate = true;
               assert onHold == null;
               onHold = packet;
               PacketImpl response = new ReplicationResponseMessage();
               channel.send(response);
               return;
            }
         }

         handler.handlePacket(packet);
      }

   }

   public static class ChannelWrapper implements Channel
   {

      private final Channel channel;

      /**
       * @param connection
       * @param id
       * @param confWindowSize
       */
      public ChannelWrapper(Channel channel)
      {
         this.channel = channel;
      }

      @Override
      public String toString()
      {
         return "ChannelWrapper(" + channel + ")";
      }

      @Override
      public long getID()
      {
         return channel.getID();
      }

      @Override
      public void send(Packet packet)
      {
         // no-op
         // channel.send(packet);
      }

      @Override
      public void sendBatched(Packet packet)
      {
         throw new UnsupportedOperationException();

      }

      @Override
      public void sendAndFlush(Packet packet)
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public Packet sendBlocking(Packet packet) throws HornetQException
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setHandler(ChannelHandler handler)
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void close()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void transferConnection(CoreRemotingConnection newConnection)
      {
         throw new UnsupportedOperationException();

      }

      @Override
      public void replayCommands(int lastConfirmedCommandID)
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public int getLastConfirmedCommandID()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void lock()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void unlock()
      {
         throw new UnsupportedOperationException();

      }

      @Override
      public void returnBlocking()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public Lock getLock()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public CoreRemotingConnection getConnection()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void confirm(Packet packet)
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setCommandConfirmationHandler(CommandConfirmationHandler handler)
      {
         throw new UnsupportedOperationException();

      }

      @Override
      public void flushConfirmations()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void handlePacket(Packet packet)
      {
         throw new UnsupportedOperationException();

      }

      @Override
      public void clearCommands()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public int getConfirmationWindowSize()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setTransferring(boolean transferring)
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean supports(byte packetID)
      {
         return true;
      }

   }
}
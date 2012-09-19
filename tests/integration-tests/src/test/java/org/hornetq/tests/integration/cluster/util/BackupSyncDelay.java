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
import org.hornetq.core.server.HornetQServer;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * An interceptor to keep a replicated backup server from reaching "up-to-date" status.
 * <p>
 * There are 3 test scenarios for 'adding data to a remote backup':<br/>
 * 1. sync data that already existed <br/>
 * 2. adding `new` Journal updates (that arrived after the backup appeared) WHILE sync'ing happens<br/>
 * 3. adding `new` Journal updates AFTER sync'ing is done<br/>
 * <p>
 * These 'withDelay' tests were created to test/verify data transfers of type 2. because there is so
 * little data, if we don't delay the sync message, we cannot be sure we are testing scenario .2.
 * because the sync will be done too fast.
 * <p>
 * One problem is that we can't add an interceptor to the backup before starting it. So we add the
 * interceptor to the 'live' which will place a different {@link ChannelHandler} in the backup
 * during the initialization of replication.
 * <p>
 * We need to hijack the replication channel handler, because we need to
 * <ol>
 * <li>send an early answer to the {@link PacketImpl#REPLICATION_SYNC_FILE} packet that signals
 * being up-to-date
 * <li>not send an answer to it, when we deliver the packet later.
 * </ol>
 */
public class BackupSyncDelay implements Interceptor
{

   private final ReplicationChannelHandler handler;
   private final HornetQServer backup;
   private final HornetQServer live;

   public void deliverUpToDateMsg()
   {
      live.getRemotingService().removeInterceptor(this);
      if (backup.isStarted())
         handler.deliver();
   }

   /**
    * @param backup
    * @param live
    * @param packetCode which packet is going to be intercepted.
    */
   public BackupSyncDelay(HornetQServer backup, HornetQServer live, byte packetCode)
   {
      assert backup.getConfiguration().isBackup();
      assert !live.getConfiguration().isBackup();
      this.backup = backup;
      this.live = live;
      live.getRemotingService().addInterceptor(this);
      handler = new ReplicationChannelHandler(packetCode);
   }

   /**
    * @param backupServer
    * @param liveServer
    */
   public BackupSyncDelay(TestableServer backupServer, TestableServer liveServer)
   {
      this(backupServer.getServer(), liveServer.getServer(), PacketImpl.REPLICATION_START_FINISH_SYNC);
   }

   @Override
   public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
   {
      if (packet.getType() == PacketImpl.BACKUP_REGISTRATION)
      {
         try
         {
            ReplicationEndpoint repEnd = backup.getReplicationEndpoint();
            handler.addSubHandler(repEnd);
            Channel repChannel = repEnd.getChannel();
            repChannel.setHandler(handler);
            handler.setChannel(repChannel);
            live.getRemotingService().removeInterceptor(this);
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

      public ReplicationChannelHandler(byte type)
      {
         this.typeToIntercept = type;
      }
      private ReplicationEndpoint handler;
      private Packet onHold;
      private Channel channel;
      public volatile boolean deliver;
      private volatile boolean delivered;
      private boolean receivedUpToDate;
      private boolean mustHold = true;
      private final byte typeToIntercept;

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

         if (typeToIntercept == PacketImpl.REPLICATION_START_FINISH_SYNC)
         {
            if (packet.getType() == PacketImpl.REPLICATION_START_FINISH_SYNC && mustHold)
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
         }
         else if (typeToIntercept == packet.getType())
         {
            channel.send(new ReplicationResponseMessage());
            return;
         }

         handler.handlePacket(packet);
      }

   }

   public static class ChannelWrapper implements Channel
   {

      private final Channel channel;

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
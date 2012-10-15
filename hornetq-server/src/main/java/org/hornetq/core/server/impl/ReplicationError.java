/**
 *
 */
package org.hornetq.core.server.impl;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInternalErrorException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.LiveNodeLocator;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * Stops the backup in case of an error at the start of Replication.
 * <p>
 * Using an interceptor for the task to avoid a server reference inside of the 'basic' channel-0
 * handler at {@link ClientSessionFactoryImpl#Channel0Handler}. As {@link ClientSessionFactoryImpl}
 * is also shipped in the HQ-client JAR (which does not include {@link HornetQServer}).
 */
final class ReplicationError implements Interceptor
{
   private final HornetQServer server;
   private LiveNodeLocator nodeLocator;

   public ReplicationError(HornetQServer server, LiveNodeLocator nodeLocator)
   {
      this.server = server;
      this.nodeLocator = nodeLocator;
   }

   @Override
   public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
   {
      if (packet.getType() != PacketImpl.BACKUP_REGISTRATION_FAILED)
         return true;
      BackupReplicationStartFailedMessage message = (BackupReplicationStartFailedMessage) packet;
      switch (message.getRegistrationProblem())
      {
         case ALREADY_REPLICATING:
            tryNext();
            break;
         case AUTHENTICATION:
            failed();
            break;
         case EXCEPTION:
            failed();
            break;
         default:
            failed();

      }
      return false;
   }

   private void failed() throws HornetQInternalErrorException
   {
      HornetQServerLogger.LOGGER.errorRegisteringBackup();
      nodeLocator.notifyRegistrationFailed(false);
   }

   private void tryNext()
   {
      nodeLocator.notifyRegistrationFailed(true);
   }

}

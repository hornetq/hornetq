/**
 *
 */
package org.hornetq.core.server.impl;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.server.HornetQServer;
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
   private static final Logger log = Logger.getLogger(ReplicationError.class);

   public ReplicationError(HornetQServer server)
   {
      this.server = server;
   }

   @Override
   public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
   {
      if (packet.getType() != PacketImpl.BACKUP_REGISTRATION_FAILED)
         return true;
      log.warn("Failed to register as backup. Stopping the server.");
      try
      {
         server.stop();
      }
      catch (Exception e)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "error trying to stop " + server, e);
      }

      return false;
   }

}

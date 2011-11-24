package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * Registers a given backup-server as the replicating backup of a live server (i.e. a regular
 * HornetQ).
 * <p>
 * If it succeeds the backup will start synchronization of its state with the new backup node, and
 * replicating any new data. If it fails the backup server will receive a message indicating
 * failure, and should shutdown.
 * @see BackupRegistrationFailedMessage
 */
public final class BackupRegistrationMessage extends PacketImpl
{

   private TransportConfiguration connector;

   private String nodeID;

   private String clusterUser;

   private String clusterPassword;

   public BackupRegistrationMessage(String nodeId, TransportConfiguration tc, String user, String password)
   {
      this();
      connector = tc;
      nodeID = nodeId;
      clusterUser = user;
      clusterPassword = password;
   }

   public BackupRegistrationMessage()
   {
      super(BACKUP_REGISTRATION);
   }

   public String getNodeID()
   {
      return nodeID;
   }

   public TransportConfiguration getConnector()
   {
      return connector;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeString(nodeID);
      buffer.writeString(clusterUser);
      buffer.writeString(clusterPassword);
      connector.encode(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      nodeID = buffer.readString();
      clusterUser = buffer.readString();
      clusterPassword = buffer.readString();
      connector = new TransportConfiguration();
      connector.decode(buffer);
   }

   /**
    * @return
    */
   public String getClusterUser()
   {
      return clusterUser;
   }

   /**
    * @return
    */
   public String getClusterPassword()
   {
      return clusterPassword;
   }

}

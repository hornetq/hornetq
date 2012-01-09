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

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((clusterPassword == null) ? 0 : clusterPassword.hashCode());
      result = prime * result + ((clusterUser == null) ? 0 : clusterUser.hashCode());
      result = prime * result + ((connector == null) ? 0 : connector.hashCode());
      result = prime * result + ((nodeID == null) ? 0 : nodeID.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
      {
         return true;
      }
      if (!super.equals(obj))
      {
         return false;
      }
      if (!(obj instanceof BackupRegistrationMessage))
      {
         return false;
      }
      BackupRegistrationMessage other = (BackupRegistrationMessage)obj;
      if (clusterPassword == null)
      {
         if (other.clusterPassword != null)
         {
            return false;
         }
      }
      else if (!clusterPassword.equals(other.clusterPassword))
      {
         return false;
      }
      if (clusterUser == null)
      {
         if (other.clusterUser != null)
         {
            return false;
         }
      }
      else if (!clusterUser.equals(other.clusterUser))
      {
         return false;
      }
      if (connector == null)
      {
         if (other.connector != null)
         {
            return false;
         }
      }
      else if (!connector.equals(other.connector))
      {
         return false;
      }
      if (nodeID == null)
      {
         if (other.nodeID != null)
         {
            return false;
         }
      }
      else if (!nodeID.equals(other.nodeID))
      {
         return false;
      }
      return true;
   }

}

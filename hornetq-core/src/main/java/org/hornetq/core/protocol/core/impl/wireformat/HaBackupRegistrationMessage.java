/**
 *
 */
package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * Registers a backup node with its live server.
 * <p>
 * After registration the live server will initiate synchronization of its state with the new backup
 * node.
 */
public class HaBackupRegistrationMessage extends PacketImpl
{

   private TransportConfiguration connector;

   private String nodeID;

   public HaBackupRegistrationMessage(String nodeId, TransportConfiguration tc)
   {
      this();
      connector = tc;
      nodeID = nodeId;
   }

   public HaBackupRegistrationMessage()
   {
      super(HA_BACKUP_REGISTRATION);
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
      connector.encode(buffer);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      nodeID = buffer.readString();
      connector = new TransportConfiguration();
      connector.decode(buffer);
   }

}

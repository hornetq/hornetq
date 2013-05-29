/**
 *
 */
package org.hornetq.api.core.client;

import java.io.Serializable;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * A member of the topology.
 * <p>
 * Each TopologyMember represents a single server and possibly any backup server that may take over
 * its duties (using the nodeId of the original server).
 */
public interface TopologyMember extends Serializable
{
   /**
    * Returns the {@code backup-group-name} of the live server and backup servers associated with
    * Topology entry.
    * <p>
    * This is a server configuration value. A (remote) backup will only work with live servers that
    * have a matching {@code backup-group-name}.
    * <p>
    * This value does not apply to "shared-storage" backup and live pairs.
    * @return the {@code backup-group-name}
    */
   public String getBackupGroupName();

   /**
    * @return configuration relative to the live server
    */
   public TransportConfiguration getLive();

   /**
    * Returns the TransportConfiguration relative to the backup server if any.
    * @return a {@link TransportConfiguration} for the backup, or null} if the live server has no
    *         backup server.
    */
   public TransportConfiguration getBackup();

   /**
    * Returns the nodeId of the server.
    * @return the nodeId
    */
   public String getNodeId();

   /**
    * @return long value representing a unique event ID
    */
   public long getUniqueEventID();

   /**
    * Returns true if this TopologyMember is the target of this remoting connection
    * @param connection
    * @return
    */
   public boolean isMember(RemotingConnection connection);

   /**
    * Returns true if this configuration is the target of this remoting connection
    * @param configuration
    * @return
    */
   public boolean isMember(TransportConfiguration configuration);

}

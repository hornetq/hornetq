/**
 *
 */
package org.hornetq.api.core.client;

import java.io.Serializable;

import org.hornetq.api.core.TransportConfiguration;

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

   public TransportConfiguration getLive();

   public TransportConfiguration getBackup();

   /**
    * Returns the nodeId of the server.
    * @return the nodeId
    */
   public String getNodeId();

   /**
    * @return
    */
   public long getUniqueEventID();
}

/*
 * Copyright 2005-2014 Red Hat, Inc.
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
    *
    * @return the {@code backup-group-name}
    */
   String getBackupGroupName();

   /**
    * Returns the {@code export-group-name} of the live server with this Topology entry.
    * <p/>
    * This is a server configuration value. a live server will only export to another live server
    * with matching {@code export-group-name}.
    * <p/>
    *
    * @return the {@code export-group-name}
    */
   String getExportGroupName();

   /**
    * @return configuration relative to the live server
    */
   TransportConfiguration getLive();

   /**
    * Returns the TransportConfiguration relative to the backup server if any.
    *
    * @return a {@link TransportConfiguration} for the backup, or null} if the live server has no
    * backup server.
    */
   TransportConfiguration getBackup();

   /**
    * Returns the nodeId of the server.
    *
    * @return the nodeId
    */
   String getNodeId();

   /**
    * @return long value representing a unique event ID
    */
   long getUniqueEventID();

   /**
    * Returns true if this TopologyMember is the target of this remoting connection
    *
    * @param connection
    * @return
    */
   boolean isMember(RemotingConnection connection);

   /**
    * Returns true if this configuration is the target of this remoting connection
    *
    * @param configuration
    * @return
    */
   boolean isMember(TransportConfiguration configuration);

}

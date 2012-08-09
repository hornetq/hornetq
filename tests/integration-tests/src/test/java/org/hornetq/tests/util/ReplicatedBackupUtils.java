/**
 *
 */
package org.hornetq.tests.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;

public final class ReplicatedBackupUtils
{
   public static final String LIVE_NODE_NAME = "hqLIVE";
   public static final String BACKUP_NODE_NAME = "hqBackup";
   private ReplicatedBackupUtils()
   {
      // Utility class
   }

   /**
    * Creates a {@link ClusterConnectionConfiguration} and adds it to the {@link Configuration}.
    * @param configuration
    * @param name
    * @param connectors
    */
   public static void createClusterConnectionConf(Configuration configuration, String name, String... connectors)
   {
      List<String> conn = new ArrayList<String>(connectors.length);
      for (String iConn : connectors)
      {
         conn.add(iConn);
      }
      ClusterConnectionConfiguration clusterConfig =
               new ClusterConnectionConfiguration("cluster1", "jms", name, 250, false, false, 1, 1, conn, false);
      configuration.getClusterConfigurations().add(clusterConfig);
   }

   public static void configureReplicationPair(Configuration backupConfig,
                                               TransportConfiguration backupConnector,
                                               TransportConfiguration backupAcceptor,
                                               Configuration liveConfig,
                                               TransportConfiguration liveConnector)
   {
      if (backupAcceptor != null)
      {
         Set<TransportConfiguration> backupAcceptorSet = backupConfig.getAcceptorConfigurations();
         backupAcceptorSet.clear();
         backupAcceptorSet.add(backupAcceptor);
      }

      backupConfig.getConnectorConfigurations().put(BACKUP_NODE_NAME, backupConnector);
      backupConfig.getConnectorConfigurations().put(LIVE_NODE_NAME, liveConnector);
      ReplicatedBackupUtils.createClusterConnectionConf(backupConfig, BACKUP_NODE_NAME, LIVE_NODE_NAME);

      backupConfig.setSharedStore(false);
      backupConfig.setBackup(true);
      backupConfig.setClustered(true);

      liveConfig.setName(LIVE_NODE_NAME);
      liveConfig.getConnectorConfigurations().put(LIVE_NODE_NAME, liveConnector);
      liveConfig.getConnectorConfigurations().put(BACKUP_NODE_NAME, backupConnector);
      liveConfig.setSecurityEnabled(false);
      liveConfig.setSharedStore(false);
      liveConfig.setClustered(true);
      ReplicatedBackupUtils.createClusterConnectionConf(liveConfig, LIVE_NODE_NAME, BACKUP_NODE_NAME);
   }
}

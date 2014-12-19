/**
 *
 */
package org.hornetq.tests.util;

import java.util.Set;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;

public final class ReplicatedBackupUtils
{
   public static final String LIVE_NODE_NAME = "hqLIVE";
   public static final String BACKUP_NODE_NAME = "hqBackup";
   private ReplicatedBackupUtils()
   {
      // Utility class
   }

   public static void configureReplicationPair(Configuration backupConfig,
                                               TransportConfiguration backupConnector,
                                               TransportConfiguration backupAcceptor,
                                               Configuration liveConfig,
                                               TransportConfiguration liveConnector,
                                               TransportConfiguration liveAcceptor)
   {
      if (backupAcceptor != null)
      {
         Set<TransportConfiguration> backupAcceptorSet = backupConfig.getAcceptorConfigurations();
         backupAcceptorSet.clear();
         backupAcceptorSet.add(backupAcceptor);
      }

      if (liveAcceptor != null)
      {
         Set<TransportConfiguration> liveAcceptorSet = liveConfig.getAcceptorConfigurations();
         liveAcceptorSet.clear();
         liveAcceptorSet.add(liveAcceptor);
      }

      backupConfig.getConnectorConfigurations().put(BACKUP_NODE_NAME, backupConnector);
      backupConfig.getConnectorConfigurations().put(LIVE_NODE_NAME, liveConnector);

      UnitTestCase.basicClusterConnectionConfig(backupConfig, BACKUP_NODE_NAME, LIVE_NODE_NAME);

      backupConfig.setSharedStore(false);
      backupConfig.setBackup(true);

      liveConfig.setName(LIVE_NODE_NAME);
      liveConfig.getConnectorConfigurations().put(LIVE_NODE_NAME, liveConnector);
      liveConfig.getConnectorConfigurations().put(BACKUP_NODE_NAME, backupConnector);
      liveConfig.setSecurityEnabled(false);
      liveConfig.setSharedStore(false);
      UnitTestCase.basicClusterConnectionConfig(liveConfig, LIVE_NODE_NAME, BACKUP_NODE_NAME);
   }
}

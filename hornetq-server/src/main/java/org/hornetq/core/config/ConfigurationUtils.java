package org.hornetq.core.config;

import org.hornetq.api.core.HornetQIllegalStateException;
import org.hornetq.core.server.HornetQServerLogger;

public final class ConfigurationUtils
{

   private ConfigurationUtils()
   {
      // Utility class
   }

   public static ClusterConnectionConfiguration getReplicationClusterConfiguration(Configuration conf) throws HornetQIllegalStateException
   {
      final String replicationCluster = conf.getReplicationClustername();
      if (replicationCluster == null || replicationCluster.isEmpty())
         return conf.getClusterConfigurations().get(0);
      for (ClusterConnectionConfiguration clusterConf : conf.getClusterConfigurations())
      {
         if (replicationCluster.equals(clusterConf.getName()))
            return clusterConf;
      }
      throw new HornetQIllegalStateException("Missing cluster-configuration for replication-cluster-name '" +
                                                replicationCluster + "'.");
   }

   // A method to check the passed Configuration object and warn users if semantically unwise parameters are present
   public static void validateConfiguration(Configuration configuration)
   {
      // Warn if connection-ttl-override/connection-ttl == check-period
      compareTTLWithCheckPeriod(configuration);
   }

   private static void compareTTLWithCheckPeriod(Configuration configuration)
   {
      for (ClusterConnectionConfiguration c : configuration.getClusterConfigurations())
         compareTTLWithCheckPeriod(c.getName(), c.getConnectionTTL(), configuration.getConnectionTTLOverride(), c.getClientFailureCheckPeriod());

      for (BridgeConfiguration c : configuration.getBridgeConfigurations())
         compareTTLWithCheckPeriod(c.getName(), c.getConnectionTTL(), configuration.getConnectionTTLOverride(), c.getClientFailureCheckPeriod());
   }

   private static void compareTTLWithCheckPeriod(String name, long connectionTTL, long connectionTTLOverride, long checkPeriod)
   {
      if (connectionTTLOverride == checkPeriod)
         HornetQServerLogger.LOGGER.connectionTTLEqualsCheckPeriod(name, "connection-ttl-override", "check-period");

      if (connectionTTL == checkPeriod)
         HornetQServerLogger.LOGGER.connectionTTLEqualsCheckPeriod(name, "connection-ttl", "check-period");
   }
}

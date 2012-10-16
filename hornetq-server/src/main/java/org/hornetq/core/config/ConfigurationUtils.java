package org.hornetq.core.config;

import org.hornetq.api.core.HornetQIllegalStateException;

public final class ConfigurationUtils
{

   private ConfigurationUtils()
   {
      // Utility class
   }

   public static ClusterConnectionConfiguration
            getReplicationClusterConfiguration(Configuration conf) throws HornetQIllegalStateException
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
}

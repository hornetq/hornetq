package org.hornetq.core.server.cluster;


import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServerLogger;

import java.lang.reflect.Array;
import java.util.List;

public class ClusterConfigurationUtil
{
   public static TransportConfiguration getTransportConfiguration(ClusterConnectionConfiguration config, Configuration configuration)
   {
      if (config.getName() == null)
      {
         HornetQServerLogger.LOGGER.clusterConnectionNotUnique();

         return null;
      }

      if (config.getAddress() == null)
      {
         HornetQServerLogger.LOGGER.clusterConnectionNoForwardAddress();

         return null;
      }

      TransportConfiguration connector = configuration.getConnectorConfigurations().get(config.getConnectorName());

      if (connector == null)
      {
         HornetQServerLogger.LOGGER.clusterConnectionNoConnector(config.getConnectorName());
         return null;
      }
      return connector;
   }

   public static DiscoveryGroupConfiguration getDiscoveryGroupConfiguration(ClusterConnectionConfiguration config, Configuration configuration)
   {
      DiscoveryGroupConfiguration dg = configuration.getDiscoveryGroupConfigurations()
            .get(config.getDiscoveryGroupName());

      if (dg == null)
      {
         HornetQServerLogger.LOGGER.clusterConnectionNoDiscoveryGroup(config.getDiscoveryGroupName());
         return null;
      }
      return dg;
   }

   public static TransportConfiguration[] getTransportConfigurations(ClusterConnectionConfiguration config, Configuration configuration)
   {
      return config.getStaticConnectors() != null ? connectorNameListToArray(config.getStaticConnectors(), configuration)
            : null;
   }

   public static TransportConfiguration[] connectorNameListToArray(final List<String> connectorNames, Configuration configuration)
   {
      TransportConfiguration[] tcConfigs = (TransportConfiguration[]) Array.newInstance(TransportConfiguration.class,
            connectorNames.size());
      int count = 0;
      for (String connectorName : connectorNames)
      {
         TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorName);

         if (connector == null)
         {
            HornetQServerLogger.LOGGER.bridgeNoConnector(connectorName);

            return null;
         }

         tcConfigs[count++] = connector;
      }

      return tcConfigs;
   }
}

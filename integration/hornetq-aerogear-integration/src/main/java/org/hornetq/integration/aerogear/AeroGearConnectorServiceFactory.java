package org.hornetq.integration.aerogear;

import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.ConnectorService;
import org.hornetq.core.server.ConnectorServiceFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;


public class AeroGearConnectorServiceFactory implements ConnectorServiceFactory
{
   @Override
   public ConnectorService createConnectorService(String connectorName, Map<String, Object> configuration, StorageManager storageManager, PostOffice postOffice, ScheduledExecutorService scheduledThreadPool)
   {
      return new AeroGearConnectorService(connectorName, configuration, postOffice, scheduledThreadPool);
   }

   @Override
   public Set<String> getAllowableProperties()
   {
      return AeroGearConstants.ALLOWABLE_PROPERTIES;
   }

   @Override
   public Set<String> getRequiredProperties()
   {
      return AeroGearConstants.REQUIRED_PROPERTIES;
   }
}

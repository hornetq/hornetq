package org.hornetq.integration.vertx;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.ConnectorService;
import org.hornetq.core.server.ConnectorServiceFactory;

public class VertxIncomingConnectorServiceFactory implements ConnectorServiceFactory
{

   @Override
   public ConnectorService createConnectorService(String connectorName,
      Map<String, Object> configuration, StorageManager storageManager,
      PostOffice postOffice, ScheduledExecutorService scheduledThreadPool)
   {

      return new IncomingVertxEventHandler(connectorName, configuration, storageManager,
               postOffice, scheduledThreadPool);

   }

   @Override
   public Set<String> getAllowableProperties()
   {
      return VertxConstants.ALLOWABLE_INCOMING_CONNECTOR_KEYS;
   }

   @Override
   public Set<String> getRequiredProperties()
   {
      return VertxConstants.REQUIRED_INCOMING_CONNECTOR_KEYS;
   }

}

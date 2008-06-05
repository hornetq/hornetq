package org.jboss.messaging.tests.unit.core.remoting.impl;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientConnectionInternal;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.jms.client.JBossConnection;

import junit.framework.TestCase;

public class INVMServerTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServerImpl server_1;
   private MessagingServerImpl server_2;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testConnectionsToTwoINVMServers() throws Exception
   {
      ClientConnectionFactory cf_1 = new ClientConnectionFactoryImpl(
            new LocationImpl(0));
      ClientConnectionInternal conn_1 = (ClientConnectionInternal) cf_1.createConnection();

      ClientConnectionFactory cf_2 = new ClientConnectionFactoryImpl(
            new LocationImpl(1));
      ClientConnectionInternal conn_2 = (ClientConnectionInternal) cf_2.createConnection();
      
      assertNotSame(conn_1.getRemotingConnection().getSessionID(), conn_2.getRemotingConnection().getSessionID());

      conn_1.close();
      conn_2.close();      
}

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      ConfigurationImpl config = new ConfigurationImpl();
      config.setServerID(0);
      config.setTransport(TransportType.INVM);
      server_1 = new MessagingServerImpl(config);
      server_1.start();

      config = new ConfigurationImpl();
      config.setServerID(1);
      config.setTransport(TransportType.INVM);
      server_2 = new MessagingServerImpl(config);
      server_2.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server_1.stop();
      server_2.stop();

      super.tearDown();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

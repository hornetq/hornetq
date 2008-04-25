package org.jboss.messaging.tests.integration;

import static org.jboss.messaging.core.remoting.TransportType.INVM;
import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;

public class CoreClientTest extends TestCase
{
   // Constants -----------------------------------------------------

   private final String QUEUE = "CoreClientTestQueue";
   // Attributes ----------------------------------------------------

   private ConfigurationImpl conf;
   private MessagingServerImpl invmServer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      conf = new ConfigurationImpl();
      conf.setInvmDisabled(false);
      conf.setTransport(INVM);
      invmServer = new MessagingServerImpl(conf);
      invmServer.start();
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      invmServer.stop();
      
      super.tearDown();
   }
   
   
   public void testINVMCoreClient() throws Exception
   {
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(new LocationImpl(0));
      ClientConnection conn = cf.createConnection();
      
      ClientSession session = conn.createClientSession(false, true, true, -1, false, false);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      
      ClientProducer producer = session.createProducer(QUEUE);

      Message message = new MessageImpl(JBossTextMessage.TYPE, false, 0,
            System.currentTimeMillis(), (byte) 1);
      message.setPayload("testINVMCoreClient".getBytes());
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(QUEUE, null, false, false, true);
      conn.start();
      
      message = consumer.receive(1000);
      assertEquals("testINVMCoreClient", new String(message.getPayload()));
      
      conn.close();
   }



   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

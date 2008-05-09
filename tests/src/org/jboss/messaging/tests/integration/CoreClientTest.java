package org.jboss.messaging.tests.integration;

import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.SimpleString;

public class CoreClientTest extends TestCase
{
   // Constants -----------------------------------------------------

   private final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");
   // Attributes ----------------------------------------------------

   private ConfigurationImpl conf;
   private MessagingServerImpl server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      conf = new ConfigurationImpl();
      conf.setTransport(TransportType.TCP);
      conf.setHost("localhost");      
      server = new MessagingServerImpl(conf);
      server.start();
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      server.stop();
      
      super.tearDown();
   }
   
   
   public void testCoreClient() throws Exception
   {
      Location location = new LocationImpl(TransportType.TCP, "localhost", ConfigurationImpl.DEFAULT_REMOTING_PORT);
            
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);
      ClientConnection conn = cf.createConnection();
      
      ClientSession session = conn.createClientSession(false, true, true, -1, false, false);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      
      ClientProducer producer = session.createProducer(QUEUE);

      ClientMessage message = new ClientMessageImpl(JBossTextMessage.TYPE, false, 0,
            System.currentTimeMillis(), (byte) 1);
      message.getBody().putString("testINVMCoreClient");
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(QUEUE, null, false, false, true);
      conn.start();
      
      message = consumer.receive(1000);
      
      assertEquals("testINVMCoreClient", message.getBody().getString());
      
      conn.close();
   }
   
   public void testCoreClientPerf() throws Exception
   {
      Location location = new LocationImpl(TransportType.TCP, "localhost", ConfigurationImpl.DEFAULT_REMOTING_PORT);
            
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);
      cf.setDefaultConsumerWindowSize(-1);
      
      ClientConnection conn = cf.createConnection();
      
      ClientSession session = conn.createClientSession(false, true, false, -1, false, false);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      
      ClientProducer producer = session.createProducer(QUEUE);

      ClientMessage message = new ClientMessageImpl(JBossTextMessage.TYPE, false, 0,
            System.currentTimeMillis(), (byte) 1);
      
      ClientConsumer consumer = session.createConsumer(QUEUE, null, false, false, true);
            
      final CountDownLatch latch = new CountDownLatch(1);
      
      final int numMessages = 100000;
      
      class MyHandler implements MessageHandler
      {
         int count;

         public void onMessage(ClientMessage msg)
         {
            count++;

            if (count == numMessages)
            {
               latch.countDown();
            }
         }            
      }

      consumer.setMessageHandler(new MyHandler());
      
      
      
      
      
      
      for (int i = 0; i < numMessages; i++)
      {      
         producer.send(message);
      }
      
//      long end = System.currentTimeMillis();
//
//      double actualRate = 1000 * (double)numMessages / ( end - start);
//                  
//      System.out.println("Rate is " + actualRate);

      conn.start();
      
      long start = System.currentTimeMillis();
      
      //start = System.currentTimeMillis();

      latch.await();
      
      long end = System.currentTimeMillis();
      
      double actualRate = 1000 * (double)numMessages / ( end - start);
      
      System.out.println("Rate is " + actualRate);
      
//      
//      message = consumer.receive(1000);
//      
//      assertEquals("testINVMCoreClient", message.getBody().getString());
//      
      conn.close();
   }



   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

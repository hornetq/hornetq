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
      conf.setSecurityEnabled(false);
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

      ClientConsumer consumer = session.createConsumer(QUEUE);
      conn.start();
      
      message = consumer.receive(1000);
      
      assertEquals("testINVMCoreClient", message.getBody().getString());
      
      conn.close();
   }
   
   public static void main(String[] args)
   {
      try
      {
         CoreClientTest test = new CoreClientTest();
         
         test.setUp();
         test.testCoreClientPerf();
         test.tearDown();
      }
      catch (Throwable t)
      {
         t.printStackTrace();
      }
   }
   
   public void testCoreClientPerf() throws Exception
   {
      Location location = new LocationImpl(TransportType.TCP, "localhost", ConfigurationImpl.DEFAULT_REMOTING_PORT);
            
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);
      cf.setDefaultConsumerWindowSize(-1);
   //   cf.setDefaultProducerMaxRate(30000);
      
      ClientConnection conn = cf.createConnection();
      
      final ClientSession session = conn.createClientSession(false, true, true, 1000, false, false);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      
      ClientProducer producer = session.createProducer(QUEUE);

      ClientMessage message = new ClientMessageImpl(JBossTextMessage.TYPE, false, 0,
            System.currentTimeMillis(), (byte) 1);
      
      byte[] bytes = new byte[1000];
      
      message.getBody().putBytes(bytes);
      
      message.getBody().flip();
      
      
      ClientConsumer consumer = session.createConsumer(QUEUE);
            
      final CountDownLatch latch = new CountDownLatch(1);
//      
      final int numMessages = 50000;
      
      class MyHandler implements MessageHandler
      {
         int count;

         public void onMessage(ClientMessage msg)
         {
            count++;
            
            try
            {
               session.acknowledge();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }

            if (count == numMessages)
            {
               latch.countDown();
            }                        
         }            
      }

      consumer.setMessageHandler(new MyHandler());
      
      
            
      //System.out.println("Waiting 10 secs");
      
     // Thread.sleep(10000);
      
      
      
      System.out.println("Starting");
      
      
      //Warmup
      for (int i = 0; i < 50000; i++)
      {      
         producer.send(message);
      }
//      
//      System.out.println("Waiting 10 secs");
//      
//      Thread.sleep(10000);
      
      
      long start = System.currentTimeMillis();
            
      for (int i = 0; i < numMessages; i++)
      {      
         producer.send(message);
      }
      
      
      
            
      //long end = System.currentTimeMillis();
      
      //double actualRate = 1000 * (double)numMessages / ( end - start);
      
      //System.out.println("Send Rate is " + actualRate);
      
//      long end = System.currentTimeMillis();
//      
//      double actualRate = 1000 * (double)numMessages / ( end - start);
      
   //   System.out.println("Rate is " + actualRate);
      
   //   conn.start();
      
     // start = System.currentTimeMillis();
      
 //     latch.await();
      
//      long end = System.currentTimeMillis();
//
//      double actualRate = 1000 * (double)numMessages / ( end - start);
//                  
//      System.out.println("Rate is " + actualRate);

      //conn.start();
      
      //System.out.println("Waiting 10 secs");
      
      
      
      long end = System.currentTimeMillis();
      
      double actualRate = 1000 * (double)numMessages / ( end - start);
      
      System.out.println("Rate is " + actualRate);
      
      //Thread.sleep(10000);
      
      
      //      conn.start();
//      
//      
//      start = System.currentTimeMillis();
//
      
//      conn.start();
////      
//      start = System.currentTimeMillis();
////      
//      latch.await();
////            
////      
//      end = System.currentTimeMillis();
//      
//      actualRate = 1000 * (double)numMessages / ( end - start);
//      
//      System.out.println("Rate is " + actualRate);
      
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

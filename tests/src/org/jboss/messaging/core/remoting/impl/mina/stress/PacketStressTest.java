/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina.stress;

import static java.util.UUID.randomUUID;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.impl.mina.integration.test.TestSupport.PORT;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.remoting.NIOConnector;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.ConfigurationHelper;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.mina.MinaConnector;
import org.jboss.messaging.core.remoting.impl.mina.MinaService;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.BytesPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class PacketStressTest extends TestCase
{

   // Constants -----------------------------------------------------

   private static final int MANY_MESSAGES = 100000;
   private static final int PAYLOAD = 10000; // in bytes
   
   // Attributes ----------------------------------------------------

   private MinaService service;
   private NIOConnector connector;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      Configuration config = ConfigurationHelper.newConfiguration(TCP, "localhost", PORT);
      service = new MinaService(config);
      service.start();
      connector = new MinaConnector(config, new PacketDispatcherImpl(null));
      
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      connector.disconnect();
      service.stop();
      
      connector = null;
      service = null;
   }
   
   public void testManyPackets() throws Exception
   {
      int spinner = MANY_MESSAGES / 100;
      System.out.println("number of messages: " + MANY_MESSAGES);
      System.out.println("message payload: " + MANY_MESSAGES + " bytes");
      System.out.println("# => " + spinner + " messages sent (1% of total messages)");
      System.out.println(". => " + spinner + " messages received (1% of total messages)");
      System.out.println();
      
      
      final long handlerID = 12346;
      CountDownLatch latch = new CountDownLatch(1);
      
      service.getDispatcher().register(new ServerHandler(handlerID, latch, spinner));
      NIOSession session = connector.connect();
      
      byte[] payloadBytes = generatePayload(PAYLOAD);
      PacketImpl packet = new BytesPacket(payloadBytes);
      packet.setTargetID(handlerID);

      long start = System.currentTimeMillis();
      for (int i = 0; i < MANY_MESSAGES; i++)
      {
       session.write(packet); 
       if (i % spinner == 0)
          System.out.print('#');
      }
      
      long durationForSending = System.currentTimeMillis() - start;      
      latch.await();
      long durationForReceiving = System.currentTimeMillis() - start;

      System.out.println();
      System.out.println(MANY_MESSAGES + " messages of " + PAYLOAD + "B sent one-way in " + durationForSending + "ms");
      System.out.println(MANY_MESSAGES + " messages of " + PAYLOAD + "B received on the server in " + durationForReceiving + "ms");
      System.out.println("==============");
      
      // in MB/s
      double sendingThroughput =  (MANY_MESSAGES * PAYLOAD) / (durationForSending * 1000); 
      double receivingThroughput =  (MANY_MESSAGES * PAYLOAD) / (durationForReceiving * 1000); 
      
      System.out.format("sending throughput: %.1f MB/s\n", sendingThroughput);
      System.out.format("receiving throughput: %.1f MB/s\n", receivingThroughput);
      System.out.println("==============");
   }

   private byte[] generatePayload(int payload)
   {
      Random rand = new Random(System.currentTimeMillis());
      byte[] bytes = new byte[payload];
      rand.nextBytes(bytes);
      return bytes;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private final class ServerHandler implements PacketHandler
   {
      private final long handlerID;
      private CountDownLatch latch;
      private int messagesReceived;
      private int spinner;

      private ServerHandler(long handlerID, CountDownLatch latch, int spinner)
      {
         this.handlerID = handlerID;
         this.latch = latch;
         this.spinner = spinner;
         messagesReceived = 0;
      }

      public long getID()
      {
         return handlerID;
      }

      public void handle(Packet packet, PacketSender sender)
      {
         messagesReceived++;
         if (messagesReceived % spinner == 0)
            System.out.print('.');
         if (messagesReceived == MANY_MESSAGES)
         {
            latch.countDown();
         }
      }
   }
}

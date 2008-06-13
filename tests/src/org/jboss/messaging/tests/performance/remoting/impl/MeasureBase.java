/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.tests.performance.remoting.impl;

import junit.framework.TestCase;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket;


/**
 * @author clebert suconic
 */
public abstract class MeasureBase extends TestCase
{
   protected RemotingServiceImpl service;
   protected PacketDispatcher serverDispatcher;

   @Override
   public void setUp() throws Exception
   {
      super.setUp();
      startServer();
      serverDispatcher.register(new FakeHandler());
   }

   @Override
   public void tearDown() throws Exception
   {
      service.stop();
   }


   public void testMixingSends() throws Throwable
   {
      RemotingConnectionImpl remoting = new RemotingConnectionImpl(getLocation(), createParameters());
      remoting.start();

      int NUMBER_OF_MESSAGES = 300;

      long start = System.currentTimeMillis();

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         if (i % 2 == 0)
         {
            remoting.sendOneWay(10, 10, new EmptyPacket(EmptyPacket.CLOSE));
         }
         else
         {
            Object ret = remoting.sendBlocking(10, 0, new EmptyPacket(EmptyPacket.CLOSE));
            assertTrue(ret instanceof EmptyPacket);
            //assertEquals(EmptyPacket.EXCEPTION, ret.getType());
         }
      }

      long end = System.currentTimeMillis();


      System.out.println("Messages / second = " + NUMBER_OF_MESSAGES * 1000 / (end - start));
      Thread.sleep(1000);

      remoting.stop();

   }

   public void testBlockSends() throws Throwable
   {
      //NIOConnector connector = createNIOConnector(new PacketDispatcherImpl(null));
      //NIOSession session = connector.connect();


      RemotingConnectionImpl remoting = new RemotingConnectionImpl(getLocation(), createParameters());
      remoting.start();

      int NUMBER_OF_MESSAGES = 100;

      long start = System.currentTimeMillis();

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         Object ret = remoting.sendBlocking(10, 10, new EmptyPacket(EmptyPacket.CLOSE));
         assertTrue(ret instanceof EmptyPacket);
      }

      long end = System.currentTimeMillis();


      System.out.println("Messages / second = " + NUMBER_OF_MESSAGES * 1000 / (end - start));
      Thread.sleep(1000);

      remoting.stop();

   }

   public void testOneWaySends() throws Throwable
   {
      //NIOConnector connector = createNIOConnector(new PacketDispatcherImpl(null));
      //NIOSession session = connector.connect();


      RemotingConnectionImpl remoting = new RemotingConnectionImpl(getLocation(), createParameters());
      remoting.start();

      int NUMBER_OF_MESSAGES = 30000;

      long start = System.currentTimeMillis();

      for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         remoting.sendOneWay(10, 10, new EmptyPacket(EmptyPacket.CLOSE));
      }

      remoting.sendBlocking(10, 10, new EmptyPacket(EmptyPacket.CLOSE));

      long end = System.currentTimeMillis();


      System.out.println("Messages / second = " + NUMBER_OF_MESSAGES * 1000 / (end - start));

      remoting.stop();

   }

   protected abstract LocationImpl getLocation();

   protected abstract ConfigurationImpl createConfiguration();


   protected void startServer() throws Exception
   {
      service = new RemotingServiceImpl(createConfiguration());
      service.start();
      serverDispatcher = service.getDispatcher();
      System.out.println("Server Dispatcher = " + serverDispatcher);
   }

   // Private

   protected ConnectionParamsImpl createParameters()
   {
      ConnectionParamsImpl param = new ConnectionParamsImpl();
      param.setTcpNoDelay(true);
      param.setBlockingCallTimeout(50000);
      return param;
   }

   // Inner Classes

   class FakeHandler implements PacketHandler
   {

      public long getID()
      {
         return 10;
      }

      public void handle(Packet packet, PacketReturner sender)
      {
         //System.out.println("Hello " + packet);
         try
         {
            if (packet.getResponseTargetID() >= 0)
            {
               packet.setTargetID(packet.getResponseTargetID());
               sender.send(packet);
            }
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }

}

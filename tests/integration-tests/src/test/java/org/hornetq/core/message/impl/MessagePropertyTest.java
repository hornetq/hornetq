package org.hornetq.core.message.impl;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

public class MessagePropertyTest extends ServiceTestBase
{
   private HornetQServer server;
   private ServerLocator locator;
   private ClientSessionFactory sf;
   private final int numMessages = 20;

   private static final String ADDRESS = "aAddress123";
   private static final SimpleString SIMPLE_STRING_KEY = new SimpleString("StringToSimpleString");

   @Override
   public void setUp() throws Exception
   {
      super.setUp();
      server = createServer(true);
      server.start();
      locator = createInVMNonHALocator();
      sf = locator.createSessionFactory();
   }

   @Override
   public void tearDown() throws Exception
   {
      try
      {
         sf.close();
         locator.close();
         server.stop();
      }
      finally
      {
         super.tearDown();
      }
   }

   private void sendMessages() throws Exception
   {
      ClientSession session = sf.createSession(true, true);
      session.createQueue(ADDRESS, ADDRESS, null, true);
      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(true);
         setBody(i, message);
         message.putIntProperty("int", i);
         message.putShortProperty("short", (short)i);
         message.putByteProperty("byte", (byte)i);
         message.putFloatProperty("float", floatValue(i));
         message.putStringProperty(SIMPLE_STRING_KEY, new SimpleString(Integer.toString(i)));
         message.putBytesProperty("byte[]", byteArray(i));
         producer.send(message);
      }
      session.commit();
   }

   private float floatValue(int i)
   {
      return (float)(i * 1.3);
   }

   private byte[] byteArray(int i)
   {
      return new byte[] { (byte)i, (byte)(i / 2) };
   }

   public void testProperties() throws Exception
   {
      sendMessages();
      receiveMessages();
   }


   private void receiveMessages() throws Exception
   {
      ClientSession session = sf.createSession(true, true);
      session.start();
      ClientConsumer consumer = session.createConsumer(ADDRESS);
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(100);
         assertNotNull("Expecting a message " + i, message);
         assertMessageBody(i, message);
         assertEquals(i, message.getIntProperty("int").intValue());
         assertEquals((short)i, message.getShortProperty("short").shortValue());
         assertEquals((byte)i, message.getByteProperty("byte").byteValue());
         assertEquals(floatValue(i), message.getFloatProperty("float").floatValue(), 0.001);
         assertEquals(new SimpleString(Integer.toString(i)),
                      message.getSimpleStringProperty(SIMPLE_STRING_KEY.toString()));
         assertEqualsByteArrays(byteArray(i), message.getBytesProperty("byte[]"));
         message.acknowledge();
      }
      assertNull("no more messages", consumer.receive(50));
      consumer.close();
      session.commit();
   }

}

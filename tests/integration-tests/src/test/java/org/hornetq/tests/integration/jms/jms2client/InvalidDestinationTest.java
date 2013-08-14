package org.hornetq.tests.integration.jms.jms2client;

import org.hornetq.tests.util.JMSTestBase;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Destination;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class InvalidDestinationTest extends JMSTestBase
{
   private JMSContext context;
   private Queue queue1;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      context = createContext();
      queue1 = createQueue(JmsContextTest.class.getSimpleName() + "Queue");
   }

   @Test
   public void invalidDestinationRuntimeExceptionTests()  throws Exception
   {
      JMSProducer producer = context.createProducer();
      Destination invalidDestination = null;
      Topic invalidTopic = null;
      String message = "hello world";
      byte[] bytesMsgSend = message.getBytes();
      Map<String, Object> mapMsgSend = new HashMap();
      mapMsgSend.put("s", "foo");
      mapMsgSend.put("b", true);
      mapMsgSend.put("i", 1);
      TextMessage expTextMessage = context.createTextMessage(message);

      try
      {
         producer.send(invalidDestination, expTextMessage);
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         producer.send(invalidDestination, message);
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      ObjectMessage om = context.createObjectMessage();
      StringBuffer sb = new StringBuffer(message);
      om.setObject(sb);
      try
      {
         producer.send(invalidDestination, om);
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         producer.send(invalidDestination, bytesMsgSend);
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         producer.send(invalidDestination, mapMsgSend);
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         context.createConsumer(invalidDestination);
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         context.createConsumer(invalidDestination, "lastMessage = TRUE");
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         context.createConsumer(invalidDestination, "lastMessage = TRUE", false);
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         context.createDurableConsumer(invalidTopic, "InvalidDestinationRuntimeException");
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         context.createDurableConsumer(invalidTopic, "InvalidDestinationRuntimeException", "lastMessage = TRUE", false);
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         context.createSharedDurableConsumer(invalidTopic, "InvalidDestinationRuntimeException");
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         context.createSharedDurableConsumer(invalidTopic, "InvalidDestinationRuntimeException", "lastMessage = TRUE");
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         context.unsubscribe("InvalidSubscriptionName");
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         context.createSharedConsumer(invalidTopic, "InvalidDestinationRuntimeException");
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }

      try
      {
         context.createSharedConsumer(invalidTopic, "InvalidDestinationRuntimeException", "lastMessage = TRUE");
      }
      catch (InvalidDestinationRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("Expected InvalidDestinationRuntimeException, received " + e);
      }
   }
   
}

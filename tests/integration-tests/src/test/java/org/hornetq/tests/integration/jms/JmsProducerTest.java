/**
 *
 */
package org.hornetq.tests.integration.jms;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;
import javax.naming.NamingException;

import org.hornetq.core.config.Configuration;
import org.hornetq.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JmsProducerTest extends JMSTestBase
{
   private JMSProducer producer;
   private Random random;
   private JMSContext context;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      context = createContext();
      producer = context.createProducer();
      random = new Random();
   }

   @Override
   protected Configuration createDefaultConfig(boolean netty) throws Exception
   {
      Configuration config = super.createDefaultConfig(netty);

      return config;
   }

   @Test
   public void testSetters()
   {
      long v = random.nextLong();
      producer.setDeliveryDelay(v);
      Assert.assertEquals(v, producer.getDeliveryDelay());

      long l = random.nextLong();
      producer.setTimeToLive(l);
      Assert.assertEquals(l, producer.getTimeToLive());

      String id = "ID: jms2-tests-correlation-id" + random.nextLong();
      producer.setJMSCorrelationID(id);
      Assert.assertEquals(id, producer.getJMSCorrelationID());
   }

   @Test
   public void testAsync() throws NamingException, Exception
   {
      CountingCompletionListener cl = new CountingCompletionListener();

      producer.setAsync(cl);
      Assert.assertEquals(cl, producer.getAsync());
      producer.setAsync(null);
      Assert.assertEquals(null, producer.getAsync());
      //
      producer.setAsync(cl);

      Queue queue1 = createQueue(JmsContextTest.class.getSimpleName() + "Queue");
      JMSConsumer consumer = context.createConsumer(queue1);
      for (int j = 0; j < 20; j++)
      {
         StringBuilder sb = new StringBuilder();
         for (int m = 0; m < 200; m++)
         {
            sb.append(random.nextLong());
         }
         Message msg = context.createTextMessage(sb.toString());
         producer.send(queue1, msg);
      }

      while (true)
      {
         Message msg3 = consumer.receive(200);
         if (msg3 == null)
            break;
         msg3.acknowledge();
      }
      context.close();
      assertTrue("completion listener should be called", cl.completionLatch.await(3, TimeUnit.SECONDS));
   }

   @Test
   public void testDisMsgID()
   {
      producer.setDisableMessageID(true);
      Assert.assertEquals(true, producer.getDisableMessageID());
      producer.setDisableMessageID(false);
      Assert.assertEquals(false, producer.getDisableMessageID());
   }

   @Test
   public void testDeliveryMode()
   {
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);
      Assert.assertEquals(DeliveryMode.PERSISTENT, producer.getDeliveryMode());
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      Assert.assertEquals(DeliveryMode.NON_PERSISTENT, producer.getDeliveryMode());
   }

   static final class CountingCompletionListener implements CompletionListener
   {

      public int completion;
      public int error;
      public CountDownLatch completionLatch = new CountDownLatch(1);

      @Override
      public void onCompletion(Message message)
      {
         completion++;
         completionLatch.countDown();
      }

      @Override
      public void onException(Message message, Exception exception)
      {
         error++;
      }

      @Override
      public String toString()
      {
         return JmsProducerTest.class.getSimpleName() + ":" + CountingCompletionListener.class.getSimpleName() + ":" +
                  completionLatch;
      }
   }
}

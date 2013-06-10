/**
 *
 */
package org.hornetq.tests.integration.jms;

import java.util.Random;

import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;

import org.hornetq.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JmsProducerTest extends JMSTestBase
{
   private JMSProducer producer;
   private Random random;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      JMSContext context = createContext();
      producer = context.createProducer();
      random = new Random();
   }

   @Test
   public void testSetters()
   {
      long v= random.nextLong();
      producer.setDeliveryDelay(v);
      Assert.assertEquals(v, producer.getDeliveryDelay());

      long l= random.nextLong();
      producer.setTimeToLive(l);
      Assert.assertEquals(l, producer.getTimeToLive());

      String id = "ID: jms2-tests-correlation-id" + random.nextLong();
      producer.setJMSCorrelationID(id);
      Assert.assertEquals(id, producer.getJMSCorrelationID());
   }


   @Test
   public void testAsync()
   {
      DummyCompletionListener cl = new DummyCompletionListener();
      producer.setAsync(cl);
      Assert.assertEquals(cl, producer.getAsync());
      producer.setAsync(null);
      Assert.assertEquals(null, producer.getAsync());
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

   static final class DummyCompletionListener implements CompletionListener
   {

      @Override
      public void onCompletion(Message message)
      {
         // noop
      }

      @Override
      public void onException(Message message, Exception exception)
      {
         // noop
      }

   }
}

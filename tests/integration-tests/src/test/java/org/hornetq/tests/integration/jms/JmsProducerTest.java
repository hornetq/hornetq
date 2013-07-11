/**
 *
 */
package org.hornetq.tests.integration.jms;

import java.util.Random;

import javax.jms.DeliveryMode;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;

import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
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
   protected void testCaseCfExtraConfig(ConnectionFactoryConfiguration configuration)
   {
      configuration.setConfirmationWindowSize(0);
      configuration.setPreAcknowledge(false);
      configuration.setBlockOnDurableSend(false);
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
}

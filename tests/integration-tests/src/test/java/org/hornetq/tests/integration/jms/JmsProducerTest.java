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

   @Test
   public void testGetNonExistentProperties() throws Exception
   {
      Throwable expected = null;

      {
         //byte
         byte value0 = 0;
         try
         {
            value0 = Byte.valueOf(null);
         }
         catch (Throwable t)
         {
            expected = t;
         }

         try
         {
            byte value1 = producer.getByteProperty("testGetNonExistentProperties");

            if (expected == null)
            {
               assertEquals("value0: " + value0 + " value1: " + value1, value1 == value0);
            }
            else
            {
               fail("non existent byte property expects exception, but got value: " + value1);
            }
         }
         catch (Throwable t)
         {
            if (expected == null) throw t;
            if (!t.getClass().equals(expected.getClass()))
            {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                  " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //boolean
         expected = null;
         boolean value0 = false;
         try
         {
            value0 = Boolean.valueOf(null);
         }
         catch (Throwable t)
         {
            expected = t;
         }

         try
         {
            boolean value1 = producer.getBooleanProperty("testGetNonExistentProperties");

            if (expected == null)
            {
               assertEquals("value0: " + value0 + " value1: " + value1, value1, value0);
            }
            else
            {
               fail("non existent boolean property expects exception, but got value: " + value1);
            }
         }
         catch (Throwable t)
         {
            if (expected == null) throw t;
            if (!t.getClass().equals(expected.getClass()))
            {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                  " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //double
         expected = null;
         double value0 = 0;
         try
         {
            value0 = Double.valueOf(null);
         }
         catch (Throwable t)
         {
            expected = t;
         }

         try
         {
            double value1 = producer.getDoubleProperty("testGetNonExistentProperties");

            if (expected == null)
            {
               assertTrue("value0: " + value0 + " value1: " + value1, value1 == value0);
            }
            else
            {
               fail("non existent double property expects exception, but got value: " + value1);
            }
         }
         catch (Throwable t)
         {
            if (expected == null) throw t;
            if (!t.getClass().equals(expected.getClass()))
            {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                  " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //float
         expected = null;
         float value0 = 0;
         try
         {
            value0 = Float.valueOf(null);
         }
         catch (Throwable t)
         {
            expected = t;
         }

         try
         {
            float value1 = producer.getFloatProperty("testGetNonExistentProperties");

            if (expected == null)
            {
               assertTrue("value0: " + value0 + " value1: " + value1, value1 == value0);
            }
            else
            {
               fail("non existent double property expects exception, but got value: " + value1);
            }
         }
         catch (Throwable t)
         {
            if (expected == null) throw t;
            if (!t.getClass().equals(expected.getClass()))
            {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                  " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //int
         expected = null;
         int value0 = 0;
         try
         {
            value0 = Integer.valueOf(null);
         }
         catch (Throwable t)
         {
            expected = t;
         }

         try
         {
            int value1 = producer.getIntProperty("testGetNonExistentProperties");

            if (expected == null)
            {
               assertTrue("value0: " + value0 + " value1: " + value1, value1 == value0);
            }
            else
            {
               fail("non existent double property expects exception, but got value: " + value1);
            }
         }
         catch (Throwable t)
         {
            if (expected == null) throw t;
            if (!t.getClass().equals(expected.getClass()))
            {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                  " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //long
         expected = null;
         long value0 = 0;
         try
         {
            value0 = Integer.valueOf(null);
         }
         catch (Throwable t)
         {
            expected = t;
         }

         try
         {
            long value1 = producer.getLongProperty("testGetNonExistentProperties");

            if (expected == null)
            {
               assertEquals("value0: " + value0 + " value1: " + value1, value1, value0);
            }
            else
            {
               fail("non existent double property expects exception, but got value: " + value1);
            }
         }
         catch (Throwable t)
         {
            if (expected == null) throw t;
            if (!t.getClass().equals(expected.getClass()))
            {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                  " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //short
         expected = null;
         short value0 = 0;
         try
         {
            value0 = Short.valueOf(null);
         }
         catch (Throwable t)
         {
            expected = t;
         }

         try
         {
            short value1 = producer.getShortProperty("testGetNonExistentProperties");

            if (expected == null)
            {
               assertTrue("value0: " + value0 + " value1: " + value1, value1 == value0);
            }
            else
            {
               fail("non existent double property expects exception, but got value: " + value1);
            }
         }
         catch (Throwable t)
         {
            if (expected == null) throw t;
            if (!t.getClass().equals(expected.getClass()))
            {
               throw new Exception("Expected exception: " + expected.getClass().getName() +
                  " but got: " + t.getClass(), t);
            }
         }
      }

      {
         //Object
         Object value0 = producer.getObjectProperty("testGetNonExistentProperties");
         assertNull(value0);
         //String
         Object value1 = producer.getStringProperty("testGetNonExistentProperties");
         assertNull(value1);
      }
   
   }
}

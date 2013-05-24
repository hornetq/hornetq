package org.hornetq.jms.tests.message;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;

public class BodyIsAssignableFromTest extends MessageBodyTestCase
{

   public void testText() throws JMSException
   {
      bodyAssignableFrom(JmsMessageType.TEXT, String.class, CharSequence.class, Comparable.class, Serializable.class);
      bodyNotAssignableFrom(JmsMessageType.TEXT, List.class, StringBuilder.class, Map.class, File.class);
   }

   public void testMap() throws JMSException
   {
      bodyAssignableFrom(JmsMessageType.MAP, Map.class, Object.class);
      bodyNotAssignableFrom(JmsMessageType.MAP, String.class, CharSequence.class, Comparable.class, Serializable.class);
   }

   public void testStream() throws JMSException
   {
      bodyNotAssignableFrom(JmsMessageType.STREAM, Object.class, Serializable.class);
   }

   public void testByte() throws JMSException
   {
      bodyAssignableFrom(JmsMessageType.BYTE, Object.class, byte[].class);
      bodyNotAssignableFrom(JmsMessageType.BYTE, String.class, CharSequence.class, Comparable.class, Serializable.class);
   }

   public void testObject() throws JMSException
   {
      bodyAssignableFrom(JmsMessageType.OBJECT, Object.class, Serializable.class,
                         Comparable.class, Double.class);
      // we are sending a Double in the body, so the de-serialized Object will be an instanceof these:
      bodyAssignableFrom(JmsMessageType.OBJECT, Comparable.class, Double.class);
      bodyNotAssignableFrom(JmsMessageType.OBJECT, String.class, CharSequence.class, List.class);
   }

   private void bodyAssignableFrom(JmsMessageType type, Class... clazz) throws JMSException
   {
      bodyAssignableFrom(type, true, clazz);
   }

   /**
    * @param type
    * @param clazz
    * @param bool
    * @throws JMSException
    */
   private void bodyAssignableFrom(final JmsMessageType type, final boolean bool, Class... clazz) throws JMSException
   {
      assertNotNull("clazz!=null", clazz);
      assertTrue("clazz[] not empty", clazz.length > 0);
      createBodySendAndReceive(type);
      Message msg = queueConsumer.receive(500);
      assertNotNull("must have a msg", msg);
      assertEquals(type.toString(), msg.getStringProperty("type"));
      for (Class<?> c : clazz)
      {
         assertEquals(msg + " " + type + " & " + c + ": " + bool, bool, msg.isBodyAssignableTo(c));
         if (bool)
         {
            Object foo = msg.getBody(c);
            assertTrue("correct type " + c, c.isInstance(foo));
         }
         else
         {
            try
            {
               Object foo = msg.getBody(c);
               fail("expected a " + MessageFormatException.class);
            }
            catch (MessageFormatException e)
            {
               // expected
            }
         }
      }
   }

   /**
    * @param type
    * @throws JMSException
    */
   private void createBodySendAndReceive(JmsMessageType type) throws JMSException
   {
      Message msg=null;
      switch (type){
         case BYTE:
            BytesMessage mByte = queueProducerSession.createBytesMessage();
            for (int i = 0; i < 20; i++)
            {
               mByte.writeByte((byte)i);
            }
            msg = mByte;
            break;
         case TEXT:
            msg = queueProducerSession.createTextMessage("JMS2");break;
         case STREAM:
            msg = queueProducerSession.createStreamMessage();
            break;
         case OBJECT:
            msg = queueProducerSession.createObjectMessage(new Double(37.6));
            break;
         case MAP:
            MapMessage msg1 = queueProducerSession.createMapMessage();
            msg1.setDouble("double", 3.45d);
            msg = msg1;
            break;
         default:
            fail("no default...");
       }
      assertNotNull(msg);
      msg.setStringProperty("type", type.toString());
      queueProducer.send(msg);
   }

   private void bodyNotAssignableFrom(JmsMessageType type, Class... clazz) throws JMSException
   {
      bodyAssignableFrom(type, false, clazz);
   }
}

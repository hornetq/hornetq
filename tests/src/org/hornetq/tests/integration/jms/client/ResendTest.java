/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.jms.client;

import java.io.Serializable;
import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.tests.util.JMSTestBase;

/**
 * Receive Messages and resend them, like the bridge would do
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ResendTest extends JMSTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Queue queue;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testResendMessage() throws Exception
   {
      Connection conn = cf.createConnection();
      try
      {
         conn.start();

         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         ArrayList<Message> msgs = new ArrayList<Message>();

         for (int i = 0; i < 10; i++)
         {
            MapMessage mm = sess.createMapMessage();
            mm.setBoolean("boolean", true);
            mm.setByte("byte", (byte)3);
            mm.setBytes("bytes", new byte[] { (byte)3, (byte)4, (byte)5 });
            mm.setChar("char", (char)6);
            mm.setDouble("double", 7.0);
            mm.setFloat("float", 8.0f);
            mm.setInt("int", 9);
            mm.setLong("long", 10l);
            mm.setObject("object", new String("this is an object"));
            mm.setShort("short", (short)11);
            mm.setString("string", "this is a string");

            msgs.add(mm);
            msgs.add(sess.createTextMessage("hello" + i));
            msgs.add(sess.createObjectMessage(new SomeSerializable("hello" + i)));
         }

         internalTestResend(msgs, sess);

      }
      finally
      {
         conn.close();
      }

   }

   public void internalTestResend(ArrayList<Message> msgs, Session sess) throws Exception
   {

      MessageProducer prod = sess.createProducer(queue);

      for (Message msg : msgs)
      {
         prod.send(msg);
      }

      sess.commit();

      MessageConsumer cons = sess.createConsumer(queue);

      for (int i = 0; i < msgs.size(); i++)
      {
         Message msg = cons.receive(5000);
         assertNotNull(msg);

         prod.send(msg);
      }

      assertNull(cons.receiveNoWait());

      sess.commit();

      for (Message originalMessage : msgs)
      {
         Message copiedMessage = cons.receive(5000);
         assertNotNull(copiedMessage);

         assertEquals(copiedMessage.getClass(), originalMessage.getClass());

         sess.commit();

         if (copiedMessage instanceof MapMessage)
         {
            MapMessage copiedMap = (MapMessage)copiedMessage;
            MapMessage originalMap = (MapMessage)originalMessage;
            assertEquals(originalMap.getString("str"), copiedMap.getString("str"));
            assertEquals(originalMap.getLong("long"), copiedMap.getLong("long"));
            assertEquals(originalMap.getInt("int"), copiedMap.getInt("int"));
            assertEquals(originalMap.getObject("object"), copiedMap.getObject("object"));
         }
         else if (copiedMessage instanceof ObjectMessage)
         {
            assertEquals(((ObjectMessage)originalMessage).getObject(), ((ObjectMessage)copiedMessage).getObject());
         }
         else if (copiedMessage instanceof TextMessage)
         {
            assertEquals(((TextMessage)originalMessage).getText(), ((TextMessage)copiedMessage).getText());
         }
      }

   }

   public static class SomeSerializable implements Serializable
   {
      /**
       * 
       */
      private static final long serialVersionUID = -8576054940441747312L;

      final String txt;

      /* (non-Javadoc)
       * @see java.lang.Object#hashCode()
       */
      @Override
      public int hashCode()
      {
         final int prime = 31;
         int result = 1;
         result = prime * result + (txt == null ? 0 : txt.hashCode());
         return result;
      }

      /* (non-Javadoc)
       * @see java.lang.Object#equals(java.lang.Object)
       */
      @Override
      public boolean equals(final Object obj)
      {
         if (this == obj)
         {
            return true;
         }
         if (obj == null)
         {
            return false;
         }
         if (getClass() != obj.getClass())
         {
            return false;
         }
         SomeSerializable other = (SomeSerializable)obj;
         if (txt == null)
         {
            if (other.txt != null)
            {
               return false;
            }
         }
         else if (!txt.equals(other.txt))
         {
            return false;
         }
         return true;
      }

      SomeSerializable(final String txt)
      {
         this.txt = txt;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      queue = createQueue("queue1");
   }

   @Override
   protected void tearDown() throws Exception
   {
      queue = null;
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

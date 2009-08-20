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

package org.hornetq.jmstests.message;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.jmstests.JMSTestCase;

/**
 *
 * @author <a href="mailto:clebert.suconic@feodorov.com">Clebert Suconic</a>
 * @version <tt>$Revision: 6220 $</tt>
 *
 * $Id: MessageHeaderTest.java 6220 2009-03-30 19:38:11Z timfox $
 */
public class LargeMessageTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSimpleLargeMessage() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(queue1);

         BytesMessage m = session.createBytesMessage();

         m.setObjectProperty("JMS_JBM_InputStream", createFakeLargeStream(1024 * 1024));

         prod.send(m);

         conn.close();

         conn = cf.createConnection();

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(queue1);

         conn.start();

         BytesMessage rm = (BytesMessage)cons.receive(10000);

         byte data[] = new byte[1024];

         System.out.println("Message = " + rm);

         for (int i = 0; i < 1024 * 1024; i += 1024)
         {
            int numberOfBytes = rm.readBytes(data);
            assertEquals(1024, numberOfBytes);
            for (int j = 0; j < 1024; j++)
            {
               assertEquals(getSamplebyte(i + j), data[j]);
            }
         }

         assertNotNull(rm);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }

   }

   public void testSimpleLargeMessage2() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(queue1);

         BytesMessage m = session.createBytesMessage();

         m.setObjectProperty("JMS_JBM_InputStream", createFakeLargeStream(10));

         prod.send(m);

         conn.close();

         conn = cf.createConnection();

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(queue1);

         conn.start();

         BytesMessage rm = (BytesMessage)cons.receive(10000);

         byte data[] = new byte[1024];

         System.out.println("Message = " + rm);

         int numberOfBytes = rm.readBytes(data);
         assertEquals(10, numberOfBytes);
         for (int j = 0; j < numberOfBytes; j++)
         {
            assertEquals(getSamplebyte(j), data[j]);
         }

         assertNotNull(rm);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }

   }

   public void testExceptionsOnSettingNonStreaming() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         TextMessage msg = session.createTextMessage();

         try
         {
            msg.setObjectProperty("JMS_JBM_InputStream", createFakeLargeStream(10));
            fail("Exception was expected");
         }
         catch (JMSException e)
         {
         }

         msg.setText("hello");

         MessageProducer prod = session.createProducer(queue1);

         prod.send(msg);

         conn.close();

         conn = cf.createConnection();

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(queue1);

         conn.start();

         TextMessage rm = (TextMessage)cons.receive(10000);

         try
         {
            rm.setObjectProperty("JMS_JBM_OutputStream", new OutputStream()
            {
               @Override
               public void write(int b) throws IOException
               {
                  System.out.println("b = " + b);
               }

            });
            fail("Exception was expected");
         }
         catch (JMSException e)
         {
         }

         
         assertEquals("hello", rm.getText());

         assertNotNull(rm);

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }

   }

   public void testWaitOnOutputStream() throws Exception
   {
      int msgSize = 1024 * 1024;

      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(queue1);

         BytesMessage m = session.createBytesMessage();

         m.setObjectProperty("JMS_JBM_InputStream", createFakeLargeStream(msgSize));

         prod.send(m);

         conn.close();

         conn = cf.createConnection();

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(queue1);

         conn.start();

         BytesMessage rm = (BytesMessage)cons.receive(10000);
         assertNotNull(rm);

         final AtomicLong numberOfBytes = new AtomicLong(0);

         final AtomicInteger numberOfErrors = new AtomicInteger(0);

         OutputStream out = new OutputStream()
         {

            int position = 0;

            @Override
            public void write(int b) throws IOException
            {
               numberOfBytes.incrementAndGet();
               if (getSamplebyte(position++) != b)
               {
                  System.out.println("Wrong byte at position " + position);
                  numberOfErrors.incrementAndGet();
               }
            }

         };

         rm.setObjectProperty("JMS_JBM_SaveStream", out);

         assertEquals(msgSize, numberOfBytes.get());

         assertEquals(0, numberOfErrors.get());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected byte getSamplebyte(final long position)
   {
      return (byte)('a' + (position) % ('z' - 'a' + 1));
   }

   // Creates a Fake LargeStream without using a real file
   protected InputStream createFakeLargeStream(final long size) throws Exception
   {
      return new InputStream()
      {
         private long count;

         private boolean closed = false;

         @Override
         public void close() throws IOException
         {
            super.close();
            System.out.println("Sent " + count + " bytes over fakeOutputStream, while size = " + size);
            closed = true;
         }

         @Override
         public int read() throws IOException
         {
            if (closed)
            {
               throw new IOException("Stream was closed");
            }
            if (count++ < size)
            {
               return getSamplebyte(count - 1);
            }
            else
            {
               return -1;
            }
         }
      };

   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   class ThreadReader extends Thread
   {
      CountDownLatch latch;

      ThreadReader(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void run()
      {
      }
   }

}

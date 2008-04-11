/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.test.messaging.jms.manual;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;


public class ManualPagingSoakTest extends MessagingTestCase
{
   protected Context ic1;
 
   protected Queue queue;
   
   protected Topic topic;
     
   protected ConnectionFactory cf;
        
   public ManualPagingSoakTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      
      Properties props1 = new Properties();
      
      props1.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
      props1.put(Context.PROVIDER_URL, "jnp://localhost:1099");
      props1.put(Context.URL_PKG_PREFIXES, "org.jnp.interfaces");
      
      ic1 = new InitialContext(props1);
                  
      queue = (Queue)ic1.lookup("queue/testQueue");
      
      topic = (Topic)ic1.lookup("topic/testTopic");
      
      cf = (ConnectionFactory)ic1.lookup("/ConnectionFactory");
      
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      ic1.close();
   }

   /** Use these attributes on testQueue for this test:

      <attribute name="FullSize">10000</attribute>
      <attribute name="PageSize">1000</attribute>
      <attribute name="DownCacheSize">1000</attribute>
    * */
   public void testPaging() throws Exception
   {
      Connection conn = null;
         
      try
      {
         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         conn.start();
         MessageConsumer cons = sess.createConsumer(queue);
         receiveMessages(cons);
         cons.close();
         cons = null;

         conn.stop();



         MessageProducer prod = sess.createProducer(queue);
         
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         final int NUM_MESSAGES = 150000;
         
         byte[] bytes = new byte[2048];
         
         String s = new String(bytes);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage(s);
            
            prod.send(tm);
            
            if (i % 1000 == 0)
            {
               log.info("Sent " + i);
            }
         }
         
         log.info("Receiving");
         
         cons = sess.createConsumer(queue);
         
         conn.start();

         int numberOfMessages = receiveMessages(cons);


         log.info("Received " + numberOfMessages + " messages");

         assertEquals(NUM_MESSAGES, numberOfMessages);   



      }
      finally
      {      
         if (conn != null) conn.close();
      }
   }

   private int receiveMessages(MessageConsumer cons)
      throws JMSException
   {
      TextMessage msg = null;

      int numberOfMessages=0;
      do
         {
            msg = (TextMessage)cons.receive(20000);

         if (msg!=null) numberOfMessages++;

         if (numberOfMessages % 1000 == 0)
         {
            log.info("Received " + numberOfMessages);
         }
      } while (msg!=null);
      return numberOfMessages;
   }
}

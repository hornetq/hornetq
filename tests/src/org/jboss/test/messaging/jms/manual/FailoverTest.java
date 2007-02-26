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

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import junit.framework.TestCase;

import org.jboss.logging.Logger;

public class FailoverTest extends TestCase
{
   private static final Logger log = Logger.getLogger(FailoverTest.class);
   

   public FailoverTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      super.setUp();      
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   public void testNOOP()
   {
      
   }
   
   public void testSendReceive() throws Exception
   {
      Hashtable properties = new Hashtable();

      properties.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");

      properties.put("java.naming.provider.url", "jnp://192.168.1.11:1199");

      properties.put("java.naming.factory.url", "org.jnp.interfaces");

      log.info("Creaing ic");

      InitialContext ic = new InitialContext(properties);

      log.info("************ REMOTE");

      Connection conn = null;

      try
      {
         log.info("Created ic");

         Queue queue = (Queue)ic.lookup("/queue/testDistributedQueue");

         log.info("Looked up queue");

         ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

         log.info("Looked up cf");

         conn = cf.createConnection();

         Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session sessCons = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = sessCons.createConsumer(queue);

         MessageListener list = new MyListener();

         cons.setMessageListener(list);

         conn.start();

         MessageProducer prod = sessSend.createProducer(queue);

         prod.setDeliveryMode(DeliveryMode.PERSISTENT);

         int count = 0;

         while (true)
         {
            TextMessage tm = sessSend.createTextMessage("message " + count);

            prod.send(tm);

            log.info("sent " + count);

            count++;

            //Thread.sleep(250);
         }


      }
      catch (Exception e)
      {
         log.error("Failed", e);
         throw e;
      }
      finally
      {
         if (conn != null)
         {
            log.info("closing connetion");
            try
            {
               conn.close();
            }
            catch (Exception ignore)
            {
            }
            log.info("closed connection");
         }
      }
   }
   
   class MyListener implements MessageListener
   {

      public void onMessage(Message msg)
      {
         try
         {
            TextMessage tm = (TextMessage)msg;
            
            log.info("Received message " + tm.getText());
         }
         catch (Exception e)
         {
            log.error("Failed to receive", e);
         }
      }
      
   }
}

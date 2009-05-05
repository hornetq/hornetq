/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A RedeliveryConsumerTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Feb 17, 2009 6:06:11 PM
 *
 *
 */
public class RedeliveryConsumerTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RedeliveryConsumerTest.class);

   
   // Attributes ----------------------------------------------------

   MessagingServer server;

   final SimpleString ADDRESS = new SimpleString("address");

   ClientSessionFactory factory;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testRedeliveryMessageStrict() throws Exception
   {
      testDedeliveryMessageOnPersistent(true);
   }

   public void testRedeliveryMessageSimpleCancel() throws Exception
   {
      testDedeliveryMessageOnPersistent(false);
   }

   public void testDeliveryNonPersistent() throws Exception
   {
      testDelivery(false);
   }

   public void testDeliveryPersistent() throws Exception
   {
      testDelivery(true);
   }

   public void testDelivery(final boolean persistent) throws Exception
   {
      setUp(true);
      ClientSession session = factory.createSession(false, false, false);
      ClientProducer prod = session.createProducer(ADDRESS);

      for (int i = 0; i < 10; i++)
      {
         prod.send(createTextMessage(session, Integer.toString(i), persistent));
      }

      session.commit();
      session.close();

      
      session = factory.createSession(null, null, false, true, true, true, 0);
      
      session.start();
      for (int loopAck = 0; loopAck < 5; loopAck++)
      {
         ClientConsumer browser = session.createConsumer(ADDRESS, null, true);
         for (int i = 0; i < 10; i++)
         {
            ClientMessage msg = browser.receive(1000);
            assertNotNull("element i=" + i + " loopAck = " + loopAck + " was expected", msg);
            msg.acknowledge();
            assertEquals(Integer.toString(i), getTextMessage(msg));
   
            // We don't change the deliveryCounter on Browser, so this should be always 0
            assertEquals(0, msg.getDeliveryCount());
         }
         
         session.commit();
         browser.close();
      }
      
      session.close();
      
      
      
      session = factory.createSession(false, false, false);
      session.start();

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      for (int loopAck = 0; loopAck < 5; loopAck++)
      {
         for (int i = 0; i < 10; i++)
         {
            ClientMessage msg = consumer.receive(1000);
            assertNotNull(msg);
            assertEquals(Integer.toString(i), getTextMessage(msg));

            // No ACK done, so deliveryCount should be always = 1
            assertEquals(1, msg.getDeliveryCount());
         }
         session.rollback();
      }

      if (persistent)
      {
         session.close();
         server.stop();
         server.start();
         session = factory.createSession(false, false, false);
         session.start();
         consumer = session.createConsumer(ADDRESS);
      }

      for (int loopAck = 1; loopAck <= 5; loopAck++)
      {
         for (int i = 0; i < 10; i++)
         {
            ClientMessage msg = consumer.receive(1000);
            assertNotNull(msg);
            msg.acknowledge();
            assertEquals(Integer.toString(i), getTextMessage(msg));
            assertEquals(loopAck, msg.getDeliveryCount());
         }
         if (loopAck < 5)
         {
            if (persistent)
            {
               session.close();
               server.stop();
               server.start();
               session = factory.createSession(false, false, false);
               session.start();
               consumer = session.createConsumer(ADDRESS);
            }
            else
            {
               session.rollback();
            }
         }
      }

      session.close();
   }

   protected void testDedeliveryMessageOnPersistent(final boolean strictUpdate) throws Exception
   {
      setUp(strictUpdate);
      ClientSession session = factory.createSession(false, false, false);
      
      log.info("created");
      
      ClientProducer prod = session.createProducer(ADDRESS);
      prod.send(createTextMessage(session, "Hello"));
      session.commit();
      session.close();

      session = factory.createSession(false, false, false);
      session.start();
      ClientConsumer consumer = session.createConsumer(ADDRESS);

      ClientMessage msg = consumer.receive(1000);
      assertEquals(1, msg.getDeliveryCount());
      session.stop();

      // if strictUpdate == true, this will simulate a crash, where the server is stopped without closing/rolling back
      // the session
      if (!strictUpdate)
      {
         // If non Strict, at least rollback/cancel should still update the delivery-counts
         session.rollback(true);
         session.close();
      }

      server.stop();

      server.start();
      
      factory = createInVMFactory();

      session = factory.createSession(false, true, false);
      session.start();
      consumer = session.createConsumer(ADDRESS);
      msg = consumer.receive(1000);
      assertNotNull(msg);
      assertEquals(2, msg.getDeliveryCount());
      session.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * @param persistDeliveryCountBeforeDelivery
    * @throws Exception
    * @throws MessagingException
    */
   private void setUp(final boolean persistDeliveryCountBeforeDelivery) throws Exception, MessagingException
   {
      Configuration config = createConfigForJournal();
      config.setJournalFileSize(10 * 1024);
      config.setJournalMinFiles(2);
      config.setSecurityEnabled(false);
      config.setPersistDeliveryCountBeforeDelivery(persistDeliveryCountBeforeDelivery);

      server = createServer(true, config);
      
      server.start();

      factory = createInVMFactory();

      ClientSession session = factory.createSession(false, false, false);
      session.createQueue(ADDRESS, ADDRESS, true);

      session.close();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (server != null && server.isStarted())
      {
         server.stop();
      }
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

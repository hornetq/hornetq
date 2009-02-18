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

package org.jboss.messaging.tests.integration.consumer;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.SimpleString;

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

   // Attributes ----------------------------------------------------

   MessagingService messagingService;

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

   protected void testDedeliveryMessageOnPersistent(boolean strictUpdate) throws Exception
   {
      setUp(strictUpdate);
      ClientSession session = factory.createSession(false, true, false);
      ClientProducer prod = session.createProducer(ADDRESS);
      prod.send(createTextMessage(session, "Hello"));
      session.commit();
      session.close();
      
      messagingService.stop();
      messagingService.start();
      
      session = factory.createSession(false, true, false);
      session.start();
      ClientConsumer consumer = session.createConsumer(ADDRESS);
      
      ClientMessage msg = consumer.receive(1000);
      assertEquals(1, msg.getDeliveryCount());
      assertNotNull(msg);
      session.stop();
      
      // if strictUpdate == true, this will simulating a crash, where the server is stopped without closing/rolling back the session
      if (!strictUpdate)
      {
         // If non Strict, at least rollback/cancel should still update the delivery-counts
         session.rollback();
         session.close();
      }
      
      messagingService.stop();
      
      messagingService.start();
      
      session = factory.createSession(false, true, false);
      session.start();
      consumer = session.createConsumer(ADDRESS);
      msg = consumer.receive(1000);
      assertNotNull(msg);
      assertEquals(2, msg.getDeliveryCount());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   /**
    * @param strictUpdateDelivery
    * @throws Exception
    * @throws MessagingException
    */
   private void setUp(boolean strictUpdateDelivery) throws Exception, MessagingException
   {
      Configuration config = createFileConfig();
      config.setJournalFileSize(10 * 1024);
      config.setJournalMinFiles(2);
      config.setSecurityEnabled(false);
      config.setStrictUpdateDelivery(strictUpdateDelivery);

      messagingService = createService(true, config);
      messagingService.start();

      factory = createInVMFactory();

      ClientSession session = factory.createSession(false, false, false);
      session.createQueue(ADDRESS, ADDRESS, true);

      session.close();
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
      if (messagingService != null && messagingService.isStarted())
      {
         messagingService.stop();
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

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

package org.jboss.test.messaging.jms.clustering;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossSession;
import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.jms.delegate.DeliveryRecovery;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.MessageProxy;
import org.jboss.jms.exception.MessagingJMSException;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class RecoverDeliveryTest extends ClusteringTestBase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public RecoverDeliveryTest(String name)
   {
      super(name);
   }
                                                        
   public void testAtomicRecover() throws Exception
   {
      JBossConnectionFactory factory =  (JBossConnectionFactory )ic[0].lookup("/ClusteredConnectionFactory");

      JBossConnection conn0 = (JBossConnection) factory.createConnection();
      assertEquals(0, getServerId(conn0));

      try
      {
         assertEquals(0, getServerId(conn0));

         JBossSession session0 = (JBossSession)conn0.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageProducer producer0 = session0.createProducer(queue[0]);
         MessageConsumer consumer0 = session0.createConsumer(queue[0]);
         conn0.start();

         for (int i=0; i<100; i++)
         {
            producer0.send(session0.createTextMessage("Message:" + i));
         }

         ArrayList list = new ArrayList();

         for (int i=0; i<10; i++)
         {
            Object msg = consumer0.receive(1000);
            list.add(msg);
         }

         conn0.close();

         conn0 = (JBossConnection) factory.createConnection();
         assertEquals(0, getServerId(conn0));

         session0 = (JBossSession)conn0.createSession(true, Session.SESSION_TRANSACTED);
         producer0 = session0.createProducer(queue[0]);
         consumer0 = session0.createConsumer(queue[0]);


         ArrayList recoveries = new ArrayList();
         for (Iterator iter = list.iterator(); iter.hasNext();)
         {
            MessageProxy msgProxy = (MessageProxy)iter.next();
            DeliveryRecovery recovery = new DeliveryRecovery(msgProxy.getDeliveryId(),
               msgProxy.getMessage().getMessageID(), queue[0].getQueueName());
            recoveries.add(recovery);
         }
         // Adding an invalid Delivery... so recoverDeliveries is supposed to fail
         recoveries.add(new DeliveryRecovery(9999,9999,queue[0].getQueueName()));


         SessionDelegate delegateSession = session0.getDelegate();

         log.info("Sending recoverDeliveries");
         try
         {
            delegateSession.recoverDeliveries(recoveries);
            fail("recoverDeliveries was supposed to fail!");
         }
         catch (MessagingJMSException e)
         {
            log.info("Receiving expected failure! -> " + e);
         }

         conn0.start();

         log.info("Receiving messages");

         // Since recoverDeliveries is supposed to be atomic now.. this is supposed to receive 100 messages
         for (int i=0; i<100; i++)
         {
            TextMessage msg = (TextMessage) consumer0.receive(1000);

            assertNotNull(msg);
            // testing order
            assertEquals("Message:" + i, msg.getText());

            log.info("Received " + msg.getText());
         }
      }
      finally
      {
         try { conn0.close();} catch (Throwable ignored){}
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 1;

      super.setUp();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }
   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}

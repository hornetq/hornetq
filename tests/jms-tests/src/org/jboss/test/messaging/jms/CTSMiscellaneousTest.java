/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.test.messaging.jms;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

import java.util.ArrayList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.InvalidSelectorException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.utils.Pair;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.test.messaging.jms.message.SimpleJMSBytesMessage;
import org.jboss.test.messaging.jms.message.SimpleJMSMessage;
import org.jboss.test.messaging.jms.message.SimpleJMSTextMessage;
import org.jboss.test.messaging.tools.container.ServiceAttributeOverrides;

/**
 * Safeguards for previously detected TCK failures.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class CTSMiscellaneousTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   protected static JBossConnectionFactory cf;

   protected ServiceAttributeOverrides overrides;

   private static final String ORG_JBOSS_MESSAGING_SERVICE_LBCONNECTION_FACTORY = "StrictTCKConnectionFactory";

   // Constructors --------------------------------------------------

   public CTSMiscellaneousTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      try
      {
         super.setUp();
         // Deploy a connection factory with load balancing but no failover on node0
         List<String> bindings = new ArrayList<String>();
         bindings.add("StrictTCKConnectionFactory");
         
         List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = 
            new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
         
         connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory"), null));
         
         List<String> jndiBindings = new ArrayList<String>();
         jndiBindings.add("/StrictTCKConnectionFactory");
         
         getJmsServerManager().createConnectionFactory("StrictTCKConnectionFactory",
                                                       connectorConfigs,
                                                       ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                       ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,  
                                                       DEFAULT_CONNECTION_TTL,
                                                       ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                       null,
                                                       ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                       ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                                                       ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                       ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                       true,
                                                       true,
                                                       true,
                                                       ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                       ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                                                       ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE,                                                       
                                                       DEFAULT_RETRY_INTERVAL,
                                                       DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                       DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                                       DEFAULT_MAX_RETRIES_AFTER_FAILOVER,
                                                       jndiBindings);

         cf = (JBossConnectionFactory)getInitialContext().lookup("/StrictTCKConnectionFactory");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

   }

   // Public --------------------------------------------------------

   /* By default we send non persistent messages asynchronously for performance reasons
    * when running with strictTCK we send them synchronously
    */
   public void testNonPersistentMessagesSentSynchronously() throws Exception
   {
      Connection c = null;

      try
      {
         c = cf.createConnection();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer p = s.createProducer(queue1);

         p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         final int numMessages = 100;

         this.assertRemainingMessages(0);

         for (int i = 0; i < numMessages; i++)
         {
            p.send(s.createMessage());
         }

         this.assertRemainingMessages(numMessages);
      }
      finally
      {
         if (c != null)
         {
            c.close();
         }

         removeAllMessages(queue1.getQueueName(), true, 0);
      }
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      undeployConnectionFactory(ORG_JBOSS_MESSAGING_SERVICE_LBCONNECTION_FACTORY);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

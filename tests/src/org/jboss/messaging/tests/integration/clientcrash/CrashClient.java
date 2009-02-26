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

package org.jboss.messaging.tests.integration.clientcrash;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;
import static org.jboss.messaging.tests.integration.clientcrash.ClientCrashTest.QUEUE;

import java.util.Arrays;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.client.JBossTextMessage;


/**
 * Code to be run in an external VM, via main().
 * 
 * This client will open a connection, receive a message and crash.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class CrashClient
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(CrashClient.class);

   // Static ---------------------------------------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      try
      {
         log.debug("args = " + Arrays.asList(args));
         
         ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory"),
                                           null,
                                           DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                           ClientCrashTest.PING_PERIOD,
                                           ClientCrashTest.CONNECTION_TTL,
                                           DEFAULT_CALL_TIMEOUT,
                                           DEFAULT_CONSUMER_WINDOW_SIZE,
                                           DEFAULT_CONSUMER_MAX_RATE,
                                           DEFAULT_SEND_WINDOW_SIZE,
                                           DEFAULT_PRODUCER_MAX_RATE,
                                           DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                           DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                           DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                           DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                           DEFAULT_AUTO_GROUP,
                                           DEFAULT_MAX_CONNECTIONS,
                                           DEFAULT_PRE_ACKNOWLEDGE,
                                           DEFAULT_ACK_BATCH_SIZE,                                 
                                           DEFAULT_RETRY_INTERVAL,
                                           DEFAULT_RETRY_INTERVAL_MULTIPLIER,                                        
                                           DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                           DEFAULT_MAX_RETRIES_AFTER_FAILOVER);
         ClientSession session = sf.createSession(false, true, true);
         ClientProducer producer = session.createProducer(QUEUE);
         
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBody().putString(ClientCrashTest.MESSAGE_TEXT_FROM_CLIENT);

         producer.send(message);
         
         // exit without closing the session properly
         System.exit(9);
      }
      catch (Throwable t)
      {
         log.error(t.getMessage(), t);
         System.exit(1);
      }
   }

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Command implementation -----------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}

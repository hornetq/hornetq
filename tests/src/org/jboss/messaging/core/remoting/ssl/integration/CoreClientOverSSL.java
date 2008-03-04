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
package org.jboss.messaging.core.remoting.ssl.integration;

import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.core.remoting.ssl.integration.CoreClientOverSSLTest.MESSAGE_TEXT_FROM_CLIENT;
import static org.jboss.messaging.core.remoting.ssl.integration.CoreClientOverSSLTest.QUEUE;

import java.util.Arrays;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.impl.RemotingConfiguration;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;

/**
 * This client will open a connection, send a message to a queue over SSL and
 * exit.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class CoreClientOverSSL
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(CoreClientOverSSL.class);

   // Static --------------------------------------------------------

   public static void main(String[] args)
   {
      try
      {
         log.info("args = " + Arrays.asList(args));

         if (args.length != 3)
         {
            log.fatal("unexpected number of args (should be 3)");
            System.exit(1);
         }

         boolean sslEnabled = Boolean.parseBoolean(args[0]);
         String keyStorePath = args[1];
         String keyStorePassword = args[2];

         RemotingConfiguration remotingConf = new RemotingConfiguration(TCP,
               "localhost", CoreClientOverSSLTest.SSL_PORT);
         remotingConf.setSSLEnabled(sslEnabled);
         remotingConf.setKeyStorePath(keyStorePath);
         remotingConf.setKeyStorePassword(keyStorePassword);

         // FIXME there should be another way to get a meaningful Version on the
         // client side...
         MessagingServer server = new MessagingServerImpl();
         ClientConnectionFactory cf = new ClientConnectionFactoryImpl(0, remotingConf, server.getVersion());
         ClientConnection conn = cf.createConnection(null, null);
         ClientSession session = conn.createClientSession(false, true, true, 0, false, false);
         ClientProducer producer = session.createProducer(QUEUE);

         MessageImpl message = new MessageImpl(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.setPayload(MESSAGE_TEXT_FROM_CLIENT.getBytes());
         producer.send(message);

         conn.close();

         System.exit(0);
      } catch (Throwable t)
      {
         log.error(t.getMessage(), t);
         System.exit(1);
      }
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

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
package org.jboss.messaging.tests.integration.core.remoting.impl;

import static org.jboss.messaging.core.remoting.TransportType.TCP;
import static org.jboss.messaging.tests.integration.core.remoting.impl.ClientExitTest.QUEUE;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;

/**
 * Code to be run in an external VM, via main().
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class GracefulClient
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(GracefulClient.class);

   // Static ---------------------------------------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      try
      {
         ConfigurationImpl config = ConfigurationHelper.newConfiguration(TCP,
               "localhost", ConfigurationImpl.DEFAULT_REMOTING_PORT);

         // FIXME there should be another way to get a meaningful Version on the
         // client side...
         MessagingServer server = new MessagingServerImpl();
         ClientConnectionFactory cf = new ClientConnectionFactoryImpl(0, config);
         ClientConnection conn = cf.createConnection(null, null);
         ClientSession session = conn.createClientSession(false, true, true, -1, false, false);
         ClientProducer producer = session.createProducer(QUEUE);
         ClientConsumer consumer = session.createConsumer(QUEUE, null, false, false, true);

         MessageImpl message = new MessageImpl(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.setPayload(ClientExitTest.MESSAGE_TEXT.getBytes());
         producer.send(message);

         conn.start();
         
         // block in receiving for 5 secs, we won't receive anything
        consumer.receive(5000);
        
        // this should silence any non-daemon thread and allow for graceful exit
        conn.close();
      } catch (Throwable t)
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

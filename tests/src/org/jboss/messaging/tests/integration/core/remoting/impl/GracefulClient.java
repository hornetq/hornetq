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
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.client.JBossTextMessage;

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
         Location config = new LocationImpl(TCP, "localhost", ConfigurationImpl.DEFAULT_REMOTING_PORT);

         ClientConnectionFactory cf = new ClientConnectionFactoryImpl(config);
         ClientConnection conn = cf.createConnection(null, null);
         ClientSession session = conn.createClientSession(false, true, true, -1, false, false);
         ClientProducer producer = session.createProducer(QUEUE);
         ClientConsumer consumer = session.createConsumer(QUEUE, null, false, false, true);

         ClientMessage message = new ClientMessageImpl(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBody().putString(ClientExitTest.MESSAGE_TEXT);
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

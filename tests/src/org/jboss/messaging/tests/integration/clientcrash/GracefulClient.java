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

import static org.jboss.messaging.tests.integration.clientcrash.ClientExitTest.QUEUE;

import org.jboss.messaging.core.client.ClientConsumer;
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
         ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory"));         
         ClientSession session = sf.createSession(false, true, true);
         ClientProducer producer = session.createProducer(QUEUE);
         ClientConsumer consumer = session.createConsumer(QUEUE);

         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBody().putString(ClientExitTest.MESSAGE_TEXT);
         producer.send(message);

         session.start();
         
         // block in receiving for 5 secs, we won't receive anything
         consumer.receive(5000);
        
         // this should silence any non-daemon thread and allow for graceful exit
         session.close();
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

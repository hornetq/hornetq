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

package org.jboss.messaging.tests.integration.ssl;

import java.util.Arrays;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.netty.TransportConstants;
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
         System.out.println("Starting******");
         
         log.debug("args = " + Arrays.asList(args));

         if (args.length != 1)
         {
            log.fatal("unexpected number of args (should be 1)");
            System.exit(1);
         }

         boolean sslEnabled = Boolean.parseBoolean(args[0]); 
         
         System.out.println("ssl enabled is " + sslEnabled);
        
         ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.netty.NettyConnectorFactory"));        
         sf.getTransportParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, sslEnabled);
         ClientSession session = sf.createSession(false, true, true);
         ClientProducer producer = session.createProducer(CoreClientOverSSLTest.QUEUE);

         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBody().putString(CoreClientOverSSLTest.MESSAGE_TEXT_FROM_CLIENT);
         producer.send(message);

         session.close();
      }
      catch (Throwable t)
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

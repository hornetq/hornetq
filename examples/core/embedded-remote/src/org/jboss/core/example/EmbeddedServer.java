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


package org.jboss.core.example;

import java.util.HashSet;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;

/**
 * A EmbeddedServer
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class EmbeddedServer
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static void main(String arg[])
   {
      try
      {
         // Step 2. Create the Configuration, and set the properties accordingly
         Configuration configuration = new ConfigurationImpl();
         configuration.setPersistenceEnabled(false);
         configuration.setSecurityEnabled(false);
         
         TransportConfiguration transpConf = new TransportConfiguration(NettyAcceptorFactory.class.getName());
         
         HashSet<TransportConfiguration> setTransp = new HashSet<TransportConfiguration>();
         setTransp.add(transpConf);
         
         configuration.setAcceptorConfigurations(setTransp);
         
         // Step 3. Create and start the server
         MessagingServer server = Messaging.newMessagingServer(configuration);
         server.start();
         System.out.println("STARTED::");
      }
      catch (Exception e)
      {
         System.out.println("FAILED::");
         e.printStackTrace();
      }

      
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

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

package org.jboss.test.jms;

import java.util.Hashtable;

import javax.naming.InitialContext;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jnp.server.Main;
import org.jnp.server.NamingBeanImpl;

/**
 * A SpawnedServer
 *
 * @author jmesnil
 * 
 */
public class SpawnedJMSServer
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      try
      {
         System.setProperty("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         System.setProperty("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");

         final NamingBeanImpl namingInfo = new NamingBeanImpl();
         namingInfo.start();
         final Main jndiServer = new Main();
         jndiServer.setNamingInfo(namingInfo);
         jndiServer.setPort(1099);
         jndiServer.setBindAddress("localhost");
         jndiServer.setRmiPort(1098);
         jndiServer.setRmiBindAddress("localhost");
         jndiServer.start();

         Configuration conf = new ConfigurationImpl();
         conf.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));
         conf.setSecurityEnabled(false);
         final MessagingServiceImpl server = Messaging.newNullStorageMessagingService(conf);
         server.start();

         JMSServerManagerImpl serverManager = JMSServerManagerImpl.newJMSServerManagerImpl(server.getServer());
         serverManager.start();

         Hashtable<String, String> env = new Hashtable<String, String>();
         env.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         env.put("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
         serverManager.setContext(new InitialContext(env));

         Runtime.getRuntime().addShutdownHook(new Thread()
         {
            @Override
            public void run()
            {
               try
               {
                  server.stop();
                  jndiServer.stop();
                  namingInfo.stop();
                  System.out.println("Server stopped");
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         });

         System.out.println("OK");

         while (true)
         {
            Thread.sleep(100);
         }
      }
      finally
      {
         System.out.println("KO");
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

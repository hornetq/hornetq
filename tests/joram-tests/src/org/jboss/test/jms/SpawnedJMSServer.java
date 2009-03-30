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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Hashtable;

import javax.naming.InitialContext;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.jms.server.JMSServerManager;
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
         System.setProperty("java.rmi.server.hostname", "localhost");
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
         final MessagingServer server = Messaging.newNullStorageMessagingServer(conf);
         server.start();

         JMSServerManager serverManager = JMSServerManagerImpl.newJMSServerManagerImpl(server);
         serverManager.start();

         Hashtable<String, String> env = new Hashtable<String, String>();
         env.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         env.put("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
         serverManager.setContext(new InitialContext(env));

         // create the reader before printing OK so that if the test is quick
         // we will still capture the STOP message sent by the client
         InputStreamReader isr = new InputStreamReader(System.in);
         BufferedReader br = new BufferedReader(isr);

         System.out.println("OK");

         String line = null;
         while ((line = br.readLine()) != null)
         {
            if ("STOP".equals(line.trim()))
            {
               server.stop();
               jndiServer.stop();
               namingInfo.stop();
               System.out.println("Server stopped");
               System.exit(0);
            } 
            else
            {
               // stop anyway but with a error status
               System.exit(1);
            }
         }
      }
      catch (Throwable t)
      {
         String allStack = t.getMessage() + "|";
         StackTraceElement[] stackTrace = t.getStackTrace();
         for (StackTraceElement stackTraceElement : stackTrace)
         {
            allStack += stackTraceElement.toString() + "|";
         }
         System.out.println(allStack);
         System.exit(1);
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

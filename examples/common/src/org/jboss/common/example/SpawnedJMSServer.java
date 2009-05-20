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
package org.jboss.common.example;

import org.jboss.messaging.integration.bootstrap.JBMBootstrapServer;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SpawnedJMSServer
{
   public static void main(String[] args)
   {
      JBMBootstrapServer bootstrap;
      try
      {
         Thread killChecker = new KillChecker(".");
         killChecker.setDaemon(true);
         killChecker.start();
         
         System.setProperty("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         System.setProperty("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
         System.setProperty("org.jboss.logging.Logger.pluginClass", "org.jboss.messaging.integration.logging.JBMLoggerPlugin");
         bootstrap = new JBMBootstrapServer(args);
         bootstrap.run();
         System.out.println("STARTED::");
      }
      catch (Throwable e)
      {
         System.out.println("FAILED::" + e.getMessage());
      }
   }
}

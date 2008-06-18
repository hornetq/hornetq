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

package org.jboss.messaging.tests.unit.core.remoting.impl;

import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.TransportType;

/**
 * 
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 * @author <a href="mailto:jeff.mesnil@jboss.com">Jeff Mesnil</a>
 *
 */
public class ConfigurationHelper
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static ConfigurationImpl newTCPConfiguration(String localhost, int port)
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.TCP);
      config.setHost(localhost);
      config.setPort(port);
      return config;
   }
   
   public static ConfigurationImpl newInVMConfig()
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.INVM);
      config.setServerID(0);
      return config;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

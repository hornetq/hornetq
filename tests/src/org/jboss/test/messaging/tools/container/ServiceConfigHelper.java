/*
 * JBoss, Home of Professional Open Source
 * Copyright 2007, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.tools.container;

import java.net.URL;

import org.jboss.test.messaging.tools.jboss.MBeanConfigurationElement;
import org.jboss.test.messaging.tools.jboss.ServiceDeploymentDescriptor;

/**
 * A helper class for loading MBean configuration files.
 *
 * @author <a href="sergey.koshcheyev@jboss.com">Sergey Koshcheyev</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class ServiceConfigHelper
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Load a service deployment descriptor from the specified file on
    * the class path.
    */
   public static ServiceDeploymentDescriptor loadConfigFile(String configFile) throws Exception
   {
      URL url = ServiceConfigHelper.class.getClassLoader().getResource(configFile);
      if (url == null)
      {
         throw new Exception("Cannot find " + configFile + " in the classpath");
      }

      return new ServiceDeploymentDescriptor(url);
   }
   
   /**
    * Load the service deployment descriptor from the specified file on
    * the class path and return the configuration element of the specified service.
    */
   public static MBeanConfigurationElement loadServiceConfiguration(String configFile, String serviceName) throws Exception
   {
      ServiceDeploymentDescriptor sdd = loadConfigFile(configFile);
      return getServiceConfiguration(sdd, serviceName);
   }
   
   public static MBeanConfigurationElement getServiceConfiguration(ServiceDeploymentDescriptor descriptor, String serviceName)
   {
      return (MBeanConfigurationElement) descriptor.query("service", serviceName).get(0);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

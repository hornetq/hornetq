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
package org.jboss.messaging.core.remoting;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.ConnectionRegistryImpl;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;

/**
 * 
 * A ConnectionRegistryLocator
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConnectionRegistryLocator
{
   private static final Logger log = Logger.getLogger(ConnectionRegistryLocator.class);
   
   public static final String CONNECTOR_FACTORIES_PROPS_FILE_NAME = "jbm-connector-factories.properties";
   
   private static ConnectionRegistry registry;
   
   static
   {
      registry = new ConnectionRegistryImpl();
      
      try
      {      
         ClassLoader loader = Thread.currentThread().getContextClassLoader();
         
         Enumeration<URL> urls = loader.getResources(CONNECTOR_FACTORIES_PROPS_FILE_NAME);
                          
         //Only read the first one - this allows user to override the defaults by placing a file
         //with the same name before it on the classpath
         if (urls.hasMoreElements())
         {
            URL url = urls.nextElement();
                   
            Properties props = new Properties();
            
            InputStream is = url.openStream();
            
            props.load(is);
            
            for (Map.Entry<Object, Object> entry: props.entrySet())
            {
               String tt = (String)entry.getKey();
               String className = (String)entry.getValue();
               TransportType transport = TransportType.valueOf(tt);
               
               try
               {
                  Class<?> clazz = loader.loadClass(className);            
                  registry.registerConnectorFactory(transport, (ConnectorFactory)clazz.newInstance());                  
               }
               catch (Exception e)
               {
                  log.warn("Failed to instantiate connector factory \"" + className + "\"", e);
               }
            }
            
            is.close();            
         }
      }
      catch (IOException e)
      {
         log.error("Failed to load acceptor factories, e");
      }
   }
   
   public static ConnectionRegistry getRegistry()
   {
      return registry;
   }
}

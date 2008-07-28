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
package org.jboss.messaging.tests.unit.core.remoting;

import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import org.jboss.messaging.core.remoting.ConnectionRegistry;
import org.jboss.messaging.core.remoting.ConnectionRegistryLocator;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.ConnectionRegistryImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A ConnectionRegistryLocatorTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConnectionRegistryLocatorTest extends UnitTestCase
{
   public void testSingleton()
   {
      ConnectionRegistry reg1 = ConnectionRegistryLocator.getRegistry();
      
      ConnectionRegistry reg2 = ConnectionRegistryLocator.getRegistry();
      
      assertTrue(reg1 == reg2);
      
      assertTrue(reg1 instanceof ConnectionRegistryImpl);
      
      assertTrue(reg2 instanceof ConnectionRegistryImpl);
   }
   
   public void testDefaultConnectorFactories() throws Exception
   {
      //The connection registry locator should have read the props file and installed
      //the default connector factories
      
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      
      Enumeration<URL> urls = loader.getResources(ConnectionRegistryLocator.CONNECTOR_FACTORIES_PROPS_FILE_NAME);
      
      ConnectionRegistry registry = ConnectionRegistryLocator.getRegistry();
                       
      while (urls.hasMoreElements())
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
            
            assertEquals(className, registry.getConnectorFactory(transport).getClass().getName());            
         }
         
         is.close();            
      }
   }
   
}

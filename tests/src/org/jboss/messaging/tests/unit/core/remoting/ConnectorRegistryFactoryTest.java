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

import org.easymock.EasyMock;
import org.jboss.messaging.core.remoting.ConnectorRegistry;
import org.jboss.messaging.core.remoting.ConnectorRegistryFactory;
import org.jboss.messaging.core.remoting.ConnectorRegistryLocator;
import org.jboss.messaging.core.remoting.impl.ConnectorRegistryImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ConnectorRegistryFactoryTest extends UnitTestCase
{

   public void testDefaultRegistry()
   {
      assertEquals(ConnectorRegistryFactory.getRegistry().getClass(), ConnectorRegistryImpl.class);
   }

   public void testSetLocator()
   {
      try
      {
         final ConnectorRegistry connectorRegistry = EasyMock.createNiceMock(ConnectorRegistry.class);
         ConnectorRegistryFactory.setRegisteryLocator(new ConnectorRegistryLocator()
         {
            public ConnectorRegistry locate()
            {
               return connectorRegistry;
            }
         });
         assertEquals(ConnectorRegistryFactory.getRegistry(), connectorRegistry);
      }
      finally
      {
         ConnectorRegistryFactory.setRegisteryLocator(null);
      }
   }

   public void testOverrideSystemProperty()
   {
      try
      {
         System.setProperty(ConnectorRegistryFactory.CONNECTOR_LOCATOR_PROPERTY, DummyConnectorRegistryLocator.class.getName());
         ConnectorRegistry connectorRegistry = EasyMock.createNiceMock(ConnectorRegistry.class);
         DummyConnectorRegistryLocator.setConnectorRegistry(connectorRegistry);
         assertEquals(ConnectorRegistryFactory.getRegistry(), connectorRegistry);
         assertEquals(ConnectorRegistryFactory.getRegistry().getClass(), DummyConnectorRegistryLocator.getConnectorRegistry().getClass());
      }
      finally
      {
         System.clearProperty(ConnectorRegistryFactory.CONNECTOR_LOCATOR_PROPERTY);
         ConnectorRegistryFactory.setRegisteryLocator(null);
      }
   }

   public void testOverrideBadSystemProperty()
   {
      try
      {
         System.setProperty(ConnectorRegistryFactory.CONNECTOR_LOCATOR_PROPERTY, "thisdoesntexist.class");
         ConnectorRegistry connectorRegistry = EasyMock.createNiceMock(ConnectorRegistry.class);
         DummyConnectorRegistryLocator.setConnectorRegistry(connectorRegistry);
         assertEquals(ConnectorRegistryFactory.getRegistry().getClass(), ConnectorRegistryImpl.class);
      }
      finally
      {
         System.clearProperty(ConnectorRegistryFactory.CONNECTOR_LOCATOR_PROPERTY);
         ConnectorRegistryFactory.setRegisteryLocator(null);
      }
   }
}

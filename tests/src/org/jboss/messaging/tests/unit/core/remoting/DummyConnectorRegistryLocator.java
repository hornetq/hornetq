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

import org.jboss.messaging.core.remoting.ConnectorRegistry;
import org.jboss.messaging.core.remoting.ConnectorRegistryLocator;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class DummyConnectorRegistryLocator implements ConnectorRegistryLocator
{
   private static ConnectorRegistry connectorRegistry;
   public ConnectorRegistry locate()
   {
      return connectorRegistry;
   }

   public static ConnectorRegistry getConnectorRegistry()
   {
      return connectorRegistry;
   }

   public static void setConnectorRegistry(ConnectorRegistry connectorRegistry)
   {
      DummyConnectorRegistryLocator.connectorRegistry = connectorRegistry;
   }
}
/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.client.Location;

import java.io.Serializable;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class LocationImpl implements Location
{

   protected TransportType transport;
   protected String host;
   protected int port = DEFAULT_REMOTING_PORT;

   public LocationImpl(TransportType transport)
   {
      assert transport == TransportType.INVM;
      this.transport = transport;
   }

   public LocationImpl(TransportType transport, String host, int port)
   {
      assert (transport == TransportType.INVM) || (host != null && port > 0);
      this.transport = transport;
      this.host = host;
      this.port = port;
   }

   public String getLocation()
   {
      return transport + (transport == TransportType.INVM?"":"://" + host + ":" + port);
   }

   public TransportType getTransport()
   {
      return transport;
   }

   public String getHost()
   {
      return host;
   }

   public int getPort()
   {
      return port;
   }
}

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
package org.jboss.messaging.core.client.impl;

import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.TransportType;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@redhat.com">Tim Fox</a>
 */
public class LocationImpl implements Location
{
	private static final long serialVersionUID = -1101852656621257742L;
	
	private TransportType transport;
	
   private String host;
   
   private int port = ConfigurationImpl.DEFAULT_PORT;
   
   public LocationImpl(final TransportType transport, final String host, final int port)
   {
      if (transport != TransportType.TCP && transport != TransportType.HTTP)
      {
         throw new IllegalArgumentException("only HTTP and TCP transports are allowed for remote location");
      }
      
      this.transport = transport;
      this.host = host;
      this.port = port;
   }
   
   public LocationImpl(final TransportType transport, final String host)
   {
      this(transport, host, ConfigurationImpl.DEFAULT_PORT);
   }

   public String getLocation()
   {
      return transport +  "://" + host + ":" + port;      
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
   
   @Override
   public String toString()
   {
      return getLocation();
   }
   
   public boolean equals(Object other)
   {
   	if (other instanceof Location == false)
   	{
   		return false;
   	}
   	Location lother = (Location)other;
   	
   	if (transport != lother.getTransport())
   	{
   	   return false;
   	}
   	
//   	if (transport == TransportType.INVM)
//   	{
//   	   return serverID == lother.getServerID();
//   	}
//   	else
//   	{
   	   return this.transport.equals(lother.getTransport()) &&
   	          this.host.equals(lother.getHost()) &&
   	          this.port == lother.getPort();
   //	}
   }
}

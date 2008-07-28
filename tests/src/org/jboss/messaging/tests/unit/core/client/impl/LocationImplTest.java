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

package org.jboss.messaging.tests.unit.core.client.impl;

import static org.jboss.messaging.core.remoting.TransportType.HTTP;
import static org.jboss.messaging.core.remoting.TransportType.TCP;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.TransportType;

/**
 * 
 * A LocationImplTest
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class LocationImplTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testTransportTypeHostPortConstructor() throws Exception
   {
      testTransportTypeHostPortConstructor(TransportType.TCP);
      testTransportTypeHostPortConstructor(TransportType.HTTP);
   }
  
   public void testTransportTypeHostConstructor() throws Exception
   {
      testTransportTypeHostConstructor(TransportType.TCP);
      testTransportTypeHostConstructor(TransportType.HTTP);
   }
     

   public void testTCPLocationEquality() throws Exception
   {
      assertEquals(new LocationImpl(TCP, "localhost", 9000), new LocationImpl(TCP, "localhost", 9000));
      assertEquals(new LocationImpl(HTTP, "localhost", 9000), new LocationImpl(HTTP, "localhost", 9000));
      assertNotSame(new LocationImpl(TCP, "localhost", 9001), new LocationImpl(TCP, "localhost", 9000));
      assertNotSame(new LocationImpl(TCP, "anotherhost", 9000), new LocationImpl(TCP, "localhost", 9000));
      assertNotSame(new LocationImpl(HTTP, "localhost", 9000), new LocationImpl(TCP, "localhost", 9000));
   }
   
   public void testSerialize() throws Exception
   {
      Location location = new LocationImpl(HTTP, "blahblah", 65126512);
      
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(location);
      oos.flush();
      
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bais);
      LocationImpl location2 = (LocationImpl)ois.readObject();
      
      assertTrue(location.equals(location2));      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void testTransportTypeHostPortConstructor(final TransportType transport) throws Exception
   {
      final String host = "uasuas";
      final int port = 1276718;
      Location location = new LocationImpl(transport, host, port);
      assertEquals(transport, location.getTransport());
      assertEquals(host, location.getHost());
      assertEquals(port, location.getPort());
   }
   
   private void testTransportTypeHostConstructor(final TransportType transport) throws Exception
   {
      final String host = "uasuas";
      Location location = new LocationImpl(transport, host);
      assertEquals(transport, location.getTransport());
      assertEquals(host, location.getHost());
      assertEquals(ConfigurationImpl.DEFAULT_PORT, location.getPort());
   }
   
   // Inner classes -------------------------------------------------
}

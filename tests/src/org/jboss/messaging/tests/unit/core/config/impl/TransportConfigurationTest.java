/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.messaging.tests.unit.core.config.impl;

import org.jboss.messaging.core.config.TransportConfiguration;

import junit.framework.TestCase;

/**
 * A TransportConfigurationTest
 *
 * @author jmesnil
 * 
 * Created 20 janv. 2009 14:46:35
 *
 *
 */
public class TransportConfigurationTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSplitNullAddress() throws Exception
   {
      String[] addresses = TransportConfiguration.splitHosts(null);
      
      assertNotNull(addresses);
      assertEquals(0, addresses.length);
   }

   public void testSplitSingleAddress() throws Exception
   {
      String[] addresses = TransportConfiguration.splitHosts("localhost");
      
      assertNotNull(addresses);
      assertEquals(1, addresses.length);
      assertEquals("localhost", addresses[0]);
   }
   
   public void testSplitManyAddresses() throws Exception
   {
      String[] addresses = TransportConfiguration.splitHosts("localhost, 127.0.0.1, 192.168.0.10");
      
      assertNotNull(addresses);
      assertEquals(3, addresses.length);
      assertEquals("localhost", addresses[0]);
      assertEquals("127.0.0.1", addresses[1]);
      assertEquals("192.168.0.10", addresses[2]);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.core.util;

import junit.framework.TestCase;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.MessagingBufferFactory;
import org.jboss.messaging.util.MessagingBufferFactoryImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MessagingBufferFactoryTest extends TestCase
{
   private MessagingBufferFactory messagingBufferFactory;
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      messagingBufferFactory = new MessagingBufferFactoryImpl();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      messagingBufferFactory = null;
   }

   public void testCreateMessagingBufferForTCP() throws Exception
   {
      int length = 512;
      MessagingBuffer buffer = messagingBufferFactory.createMessagingBuffer(TransportType.TCP, length);
      assertNotNull(buffer);
      assertEquals(length, buffer.capacity());
   }

   
   public void testCreateMessagingBufferForINVM() throws Exception
   {
      int length = 512;
      MessagingBuffer buffer = messagingBufferFactory.createMessagingBuffer(TransportType.INVM, length);
      assertNotNull(buffer);
      assertEquals(length, buffer.capacity());
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

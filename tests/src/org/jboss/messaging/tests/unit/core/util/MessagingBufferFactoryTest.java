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

import java.nio.ByteBuffer;

import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.mina.IoBufferWrapper;
import org.jboss.messaging.util.ByteBufferWrapper;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.MessagingBufferFactory;

import junit.framework.TestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MessagingBufferFactoryTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCreateMessagingBufferForTCP() throws Exception
   {
      int length = 512;
      MessagingBuffer buffer = MessagingBufferFactory.createMessagingBuffer(TransportType.TCP, length);
      assertNotNull(buffer);
      assertEquals(length, buffer.capacity());
   }

   
   public void testCreateMessagingBufferForINVM() throws Exception
   {
      int length = 512;
      MessagingBuffer buffer = MessagingBufferFactory.createMessagingBuffer(TransportType.INVM, length);
      assertNotNull(buffer);
      assertEquals(length, buffer.capacity());
   }
   
   public void testCreateMessagingBufferFromByteBufferWrapper() throws Exception
   {
      int length = 512;
      MessagingBuffer buffer = new ByteBufferWrapper(ByteBuffer.allocate(length));
      
      MessagingBuffer buff = MessagingBufferFactory.createMessagingBuffer(buffer, length);
      assertNotNull(buff);
      assertTrue(buff instanceof ByteBufferWrapper);
   }

   public void testCreateMessagingBufferFromIoBufferWrapper() throws Exception
   {
      int length = 512;
      MessagingBuffer buffer = new IoBufferWrapper(length);
      
      MessagingBuffer buff = MessagingBufferFactory.createMessagingBuffer(buffer, length);
      assertNotNull(buff);
      assertTrue(buff instanceof IoBufferWrapper);
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

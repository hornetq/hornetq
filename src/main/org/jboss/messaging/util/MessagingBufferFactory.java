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
package org.jboss.messaging.util;

import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.mina.IoBufferWrapper;

import java.nio.ByteBuffer;

/**
 * a factory class for creating an appropriate type of MessagingBuffer.
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MessagingBufferFactory
{
   public static MessagingBuffer createMessagingBuffer(TransportType transportType, int len)
   {
      if(transportType == TransportType.TCP)
      {
         return new IoBufferWrapper(len);
      }
      else if(transportType == TransportType.INVM)
      {
         return new ByteBufferWrapper(ByteBuffer.allocate(len));
      }
      else
      {
         throw new IllegalArgumentException("No Messaging Buffer for transport");
      }
   }

   public static MessagingBuffer createMessagingBuffer(MessagingBuffer buffer, int len)
   {
      if(buffer instanceof IoBufferWrapper)
      {
         return new IoBufferWrapper(len);
      }
      else if(buffer instanceof ByteBufferWrapper)
      {
         return new ByteBufferWrapper(ByteBuffer.allocate(len));
      }
      else
      {
         throw new IllegalArgumentException("No Messaging Buffer for transport");
      }
   }
}

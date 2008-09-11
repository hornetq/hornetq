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


package org.jboss.messaging.tests.unit.core.journal.impl.fakes;

import org.jboss.messaging.core.journal.EncodingSupport;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class ByteArrayEncoding implements EncodingSupport
{
   
   // Constants -----------------------------------------------------
   final byte[] data;
   
  
   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   public ByteArrayEncoding(final byte[] data)
   {
      this.data = data;
   }
   
  
   // Public --------------------------------------------------------

   public void decode(final MessagingBuffer buffer)
   {
      throw new IllegalStateException("operation not supported");
   }

   public void encode(final MessagingBuffer buffer)
   {
      buffer.putBytes(data);
   }

   public int getEncodeSize()
   {
      return data.length;
   }
   

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}

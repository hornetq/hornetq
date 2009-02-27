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
 * Provides a SimpleEncoding with a Fake Payload
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class SimpleEncoding implements EncodingSupport
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   private final int size;

   private final byte bytetosend;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SimpleEncoding(final int size, final byte bytetosend)
   {
      this.size = size;
      this.bytetosend = bytetosend;
   }

   // Public --------------------------------------------------------
   public void decode(final MessagingBuffer buffer)
   {
      throw new UnsupportedOperationException();

   }

   public void encode(final MessagingBuffer buffer)
   {
      for (int i = 0; i < size; i++)
      {
         buffer.writeByte(bytetosend);
      }
   }

   public int getEncodeSize()
   {
      return size;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

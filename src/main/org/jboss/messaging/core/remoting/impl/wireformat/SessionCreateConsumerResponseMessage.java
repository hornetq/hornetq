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

package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateConsumerResponseMessage extends DuplicablePacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int windowSize;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerResponseMessage(final int windowSize)
   {
      super(SESS_CREATECONSUMER_RESP);

      this.windowSize = windowSize;
   }
   
   public SessionCreateConsumerResponseMessage()
   {
      super(SESS_CREATECONSUMER_RESP);
   }

   // Public --------------------------------------------------------

   public boolean isResponse()
   {
      return true;
   }
   
   public int getWindowSize()
   {
   	return windowSize;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      super.encodeBody(buffer);
      buffer.putInt(windowSize);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      super.decodeBody(buffer);
      windowSize = buffer.getInt();
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", windowSize=" + windowSize);
      buf.append("]");
      return buf.toString();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionCreateConsumerResponseMessage == false)
      {
         return false;
      }
            
      SessionCreateConsumerResponseMessage r = (SessionCreateConsumerResponseMessage)other;
      
      return super.equals(other) && this.windowSize == r.windowSize;        
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

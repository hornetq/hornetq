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

import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class Pong extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long sessionID;

   private boolean sessionFailed;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public Pong(final long sessionID, final boolean sessionFailed)
   {
      super(PONG);

      this.sessionID = sessionID;
      this.sessionFailed = sessionFailed;
   }
   
   public Pong()
   {
      super(PONG);
   }

   // Public --------------------------------------------------------

   public long getSessionID()
   {
      return sessionID;
   }

   public boolean isSessionFailed()
   {
      return sessionFailed;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(sessionID);
      buffer.putBoolean(sessionFailed);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      sessionID = buffer.getLong();
      sessionFailed = buffer.getBoolean();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", sessionID=" + sessionID + ", sessionFailed=" + sessionFailed + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

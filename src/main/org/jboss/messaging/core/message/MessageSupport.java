/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.message;

import org.jboss.messaging.core.Message;

import java.io.Serializable;
import java.util.Map;

/**
 * A simple Message implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class MessageSupport extends RoutableSupport implements Message
{
   // Attributes ----------------------------------------------------

   protected Serializable payload;

   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public MessageSupport()
   {
   }

   /**
    * @param messageID
    */
   public MessageSupport(Serializable messageID)
   {
      super(messageID);
   }

   public MessageSupport(Serializable messageID, Serializable payload)
   {
      super(messageID);
      this.payload = payload;
   }
   public MessageSupport(Serializable messageID, boolean reliable, Serializable payload)
   {
      this(messageID, reliable, Long.MAX_VALUE, payload);
   }

   public MessageSupport(Serializable messageID, boolean reliable)
   {
      this(messageID, reliable, Long.MAX_VALUE, null);
   }

   public MessageSupport(Serializable messageID, boolean reliable, long timeToLive)
   {
      this(messageID, reliable, timeToLive, null);
   }

   public MessageSupport(Serializable messageID,
                         boolean reliable,
                         long timeToLive,
                         Serializable payload)
   {
      super(messageID, reliable, timeToLive);
      this.payload = payload;
   }

   public MessageSupport(Serializable messageID,
                         boolean reliable,
                         long expiration,
                         long timestamp,
                         Map headers,
                         Serializable payload)
   {
      super(messageID, reliable, expiration, timestamp, headers);
      this.payload = payload;
   }

   protected MessageSupport(MessageSupport other)
   {
      super(other);
      this.payload = other.payload;
   }

   // Routable implementation ---------------------------------------

   public Message getMessage()
   {
      return this;
   }

   // Message implementation ----------------------------------------

   public boolean isReference()
   {
      return false;
   }

   public Serializable getPayload()
   {
      return payload;
   }

   // Public --------------------------------------------------------

   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }
      if (!(o instanceof MessageSupport))
      {
         return false;
      }
      MessageSupport that = (MessageSupport)o;
      if (messageID == null)
      {
         return that.messageID == null;
      }
      return messageID.equals(that.messageID);
   }

   /**
    * @return a reference of the internal header map.
    */
   public Map getHeaders()
   {
      return headers;
   }


   public int hashCode()
   {
      if (messageID == null)
      {
         return 0;
      }
      return messageID.hashCode();
   }

   public String toString()
   {
      return "M["+messageID+"]";
   }
}

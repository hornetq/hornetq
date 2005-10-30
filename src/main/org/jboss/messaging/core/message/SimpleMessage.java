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


import java.io.Serializable;

/**
 * A simple Message implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class SimpleMessage extends MessageSupport
{
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public SimpleMessage()
   {
   }

   /**
    * @param messageID
    */
   SimpleMessage(Serializable messageID)
   {
      super(messageID);
   }

   SimpleMessage(Serializable messageID, Serializable payload)
   {
      super(messageID, payload);
   }

   SimpleMessage(Serializable messageID, boolean reliable, Serializable payload)
   {
      super(messageID, reliable, payload);
   }

   SimpleMessage(Serializable messageID, boolean reliable)
   {
      super(messageID, reliable);
   }

   SimpleMessage(Serializable messageID, boolean reliable, long timeToLive)
   {
      super(messageID, reliable, timeToLive);
   }

   public SimpleMessage(Serializable messageID, boolean reliable, long timeToLive,
                         Serializable payload)
   {
      super(messageID, reliable, timeToLive, payload);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "SimpleMessage["+messageID+"]";
   }
}

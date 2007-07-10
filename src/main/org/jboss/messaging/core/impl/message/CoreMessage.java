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
package org.jboss.messaging.core.impl.message;

import java.util.Map;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2202 $</tt>
 *
 * $Id: CoreMessage.java 2202 2007-02-08 10:50:26Z timfox $
 */
public class CoreMessage extends MessageSupport
{
   // Constants -----------------------------------------------------

	private static final long serialVersionUID = -4740357138097778538L;
	
	public static final byte TYPE = 127;

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public CoreMessage()
   {
   }
   
   public CoreMessage(long messageID,
                      boolean reliable,
                      long expiration,
                      long timestamp,
                      byte priority,
                      Map headers,
                      byte[] payload)
   {
      super(messageID, reliable, expiration, timestamp, priority, headers, payload);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "CoreMessage["+messageID+"]";
   }
   
   public byte getType()
   {
      return TYPE;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

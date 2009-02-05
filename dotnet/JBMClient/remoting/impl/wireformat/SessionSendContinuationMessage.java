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
import org.jboss.messaging.util.DataConstants;


/**
 * A SessionSendContinuationMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 4, 2008 12:25:14 PM
 *
 *
 */
public class SessionSendContinuationMessage extends SessionContinuationMessage
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean requiresResponse;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * @param type
    */
   public SessionSendContinuationMessage()
   {
      super(SESS_SEND_CONTINUATION);
   }

   /**
    * @param type
    * @param body
    * @param continues
    * @param requiresResponse
    */
   public SessionSendContinuationMessage(final byte[] body,
                                         final boolean continues,
                                         final boolean requiresResponse)
   {
      super(SESS_SEND_CONTINUATION, body, continues);
      this.requiresResponse = requiresResponse;
   }


   // Public --------------------------------------------------------
   
   /**
    * @return the requiresResponse
    */
   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }

   @Override
   public int getRequiredBufferSize()
   {
      return super.getRequiredBufferSize() + DataConstants.SIZE_BOOLEAN;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      super.encodeBody(buffer);
      buffer.putBoolean(requiresResponse);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      super.decodeBody(buffer);
      requiresResponse = buffer.getBoolean();
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

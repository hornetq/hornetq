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

import org.jboss.messaging.core.remoting.MessagingBuffer;

/**
 * 
 * A ProducerFlowCreditMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ProducerFlowCreditMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int credits;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ProducerFlowCreditMessage(final int credits)
   {
      super(PROD_RECEIVETOKENS);

      this.credits = credits;
   }
   
   public ProducerFlowCreditMessage()
   {
      super(PROD_RECEIVETOKENS);
   }

   // Public --------------------------------------------------------

   public int getTokens()
   {
      return credits;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(credits);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      credits = buffer.getInt();
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", credits=" + credits);
      buf.append("]");
      return buf.toString();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof ProducerFlowCreditMessage == false)
      {
         return false;
      }
            
      ProducerFlowCreditMessage r = (ProducerFlowCreditMessage)other;
      
      return super.equals(other) && this.credits == r.credits;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.protocol.core.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.PacketImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 */
public class SessionRequestProducerCreditsMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int credits;

   private SimpleString address;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionRequestProducerCreditsMessage(final int credits, final SimpleString address)
   {
      super(PacketImpl.SESS_PRODUCER_REQUEST_CREDITS);

      this.credits = credits;

      this.address = address;
   }

   public SessionRequestProducerCreditsMessage()
   {
      super(PacketImpl.SESS_PRODUCER_REQUEST_CREDITS);
   }

   // Public --------------------------------------------------------

   public int getCredits()
   {
      return credits;
   }

   public SimpleString getAddress()
   {
      return address;
   }

   // public boolean isRequiresConfirmations()
   // {
   // return false;
   // }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(credits);
      buffer.writeSimpleString(address);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      credits = buffer.readInt();
      address = buffer.readSimpleString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

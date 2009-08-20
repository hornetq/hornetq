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

package org.hornetq.core.remoting.impl.wireformat;

import javax.transaction.xa.Xid;

import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionXAPrepareMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Xid xid;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAPrepareMessage(final Xid xid)
   {
      super(SESS_XA_PREPARE);

      this.xid = xid;
   }

   public SessionXAPrepareMessage()
   {
      super(SESS_XA_PREPARE);
   }

   // Public --------------------------------------------------------

   public Xid getXid()
   {
      return xid;
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + XidCodecSupport.getXidEncodeLength(xid);
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      XidCodecSupport.encodeXid(xid, buffer);
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
      xid = XidCodecSupport.decodeXid(buffer);
   }

   public boolean equals(Object other)
   {
      if (other instanceof SessionXAPrepareMessage == false)
      {
         return false;
      }

      SessionXAPrepareMessage r = (SessionXAPrepareMessage)other;

      return super.equals(other) && this.xid.equals(r.xid);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

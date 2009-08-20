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
public class SessionXACommitMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean onePhase;

   private Xid xid;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXACommitMessage(final Xid xid, final boolean onePhase)
   {
      super(SESS_XA_COMMIT);

      this.xid = xid;
      this.onePhase = onePhase;
   }

   public SessionXACommitMessage()
   {
      super(SESS_XA_COMMIT);
   }

   // Public --------------------------------------------------------

   public Xid getXid()
   {
      return xid;
   }

   public boolean isOnePhase()
   {
      return onePhase;
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + XidCodecSupport.getXidEncodeLength(xid) + DataConstants.SIZE_BOOLEAN;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      XidCodecSupport.encodeXid(xid, buffer);
      buffer.writeBoolean(onePhase);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      xid = XidCodecSupport.decodeXid(buffer);
      onePhase = buffer.readBoolean();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", xid=" + xid + ", onePhase=" + onePhase + "]";
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionXACommitMessage == false)
      {
         return false;
      }

      SessionXACommitMessage r = (SessionXACommitMessage)other;

      return super.equals(other) && xid.equals(r.xid) && onePhase == r.onePhase;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

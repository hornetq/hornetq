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

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;


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
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      XidCodecSupport.encodeXid(xid, buffer);
      buffer.putBoolean(onePhase);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      xid = XidCodecSupport.decodeXid(buffer);
      onePhase = buffer.getBoolean();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", xid=" + xid + ", onePhase=" + onePhase + "]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionXACommitMessage == false)
      {
         return false;
      }
            
      SessionXACommitMessage r = (SessionXACommitMessage)other;
      
      return super.equals(other) && this.xid.equals(r.xid) &&
             this.onePhase == r.onePhase;
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


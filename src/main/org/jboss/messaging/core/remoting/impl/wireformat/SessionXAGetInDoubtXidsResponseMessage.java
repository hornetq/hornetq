/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.util.MessagingBuffer;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionXAGetInDoubtXidsResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private List<Xid> xids;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAGetInDoubtXidsResponseMessage(final List<Xid> xids)
   {
      super(SESS_XA_INDOUBT_XIDS_RESP);
      
      this.xids = xids;
   }
   
   public SessionXAGetInDoubtXidsResponseMessage()
   {
      super(SESS_XA_INDOUBT_XIDS_RESP);
   }

   // Public --------------------------------------------------------
   
   public List<Xid> getXids()
   {
      return xids;
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(xids.size());

      for (Xid xid: xids)
      {
         XidCodecSupport.encodeXid(xid, buffer);
      }    
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      int len = buffer.getInt();
      xids = new ArrayList<Xid>(len);      
      for (int i = 0; i < len; i++)
      {
         Xid xid = XidCodecSupport.decodeXid(buffer);
         
         xids.add(xid);
      }      
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}



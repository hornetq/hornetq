/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SessionXAGetInDoubtXidsResponse;


/**
 * 
 * A SessionXAGetInDoubtXidsResponseCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAGetInDoubtXidsResponseCodec extends AbstractPacketCodec<SessionXAGetInDoubtXidsResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAGetInDoubtXidsResponseCodec()
   {
      super(PacketType.REQ_XA_INDOUBT_XIDS);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionXAGetInDoubtXidsResponse message, RemotingBuffer out) throws Exception
   {      
      int bodyLength = 1;
      
      for (Xid xid: message.getXids())
      {
         bodyLength += getXidLength(xid);
      }
       
      out.putInt(bodyLength);
      
      out.putInt(message.getXids().size());
      
      for (Xid xid: message.getXids())
      {
        encodeXid(xid, out);
      }
      
   }

   @Override
   protected SessionXAGetInDoubtXidsResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      
      int size = in.getInt();
      
      List<Xid> xids = new ArrayList<Xid>(size);
      
      for (int i = 0; i < size; i++)
      {
         Xid xid = decodeXid(in);
         
         xids.add(xid);
      }
      
      return new SessionXAGetInDoubtXidsResponse(xids);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


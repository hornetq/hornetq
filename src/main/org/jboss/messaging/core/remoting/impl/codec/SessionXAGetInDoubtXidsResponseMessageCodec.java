/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.util.DataConstants.SIZE_INT;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionXAGetInDoubtXidsResponseMessage;
import org.jboss.messaging.util.DataConstants;


/**
 * 
 * A SessionXAGetInDoubtXidsResponseMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionXAGetInDoubtXidsResponseMessageCodec extends AbstractPacketCodec<SessionXAGetInDoubtXidsResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAGetInDoubtXidsResponseMessageCodec()
   {
      super(PacketType.SESS_XA_INDOUBT_XIDS_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   public int getBodyLength(final SessionXAGetInDoubtXidsResponseMessage packet) throws Exception
   {   	
      int bodyLength = SIZE_INT;
      
      for (Xid xid: packet.getXids())
      {
         bodyLength += getXidLength(xid);
      }
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionXAGetInDoubtXidsResponseMessage message, final RemotingBuffer out) throws Exception
   {      
      out.putInt(message.getXids().size());
      
      for (Xid xid: message.getXids())
      {
        encodeXid(xid, out);
      }      
   }

   @Override
   protected SessionXAGetInDoubtXidsResponseMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      int size = in.getInt();
      
      List<Xid> xids = new ArrayList<Xid>(size);
      
      for (int i = 0; i < size; i++)
      {
         Xid xid = decodeXid(in);
         
         xids.add(xid);
      }
      
      return new SessionXAGetInDoubtXidsResponseMessage(xids);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


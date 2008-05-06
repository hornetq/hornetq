/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.util.DataConstants.SIZE_BOOLEAN;
import static org.jboss.messaging.util.DataConstants.SIZE_LONG;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionCancelMessage;
import org.jboss.messaging.util.DataConstants;

/**
 * 
 * A SessionCancelMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionCancelMessageCodec extends AbstractPacketCodec<SessionCancelMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCancelMessageCodec()
   {
      super(PacketType.SESS_CANCEL);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   public int getBodyLength(final SessionCancelMessage packet) throws Exception
   {   	
      return SIZE_LONG + SIZE_BOOLEAN;
   }
   
   @Override
   protected void encodeBody(final SessionCancelMessage message, final RemotingBuffer out) throws Exception
   {
      out.putLong(message.getDeliveryID());
      out.putBoolean(message.isExpired());
   }

   @Override
   protected SessionCancelMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      long deliveryID = in.getLong();
      boolean expired = in.getBoolean();
     
      return new SessionCancelMessage(deliveryID, expired);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_ADD_DESTINATION;
import static org.jboss.messaging.util.DataConstants.SIZE_BOOLEAN;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;
import org.jboss.messaging.util.DataConstants;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A SessionAddDestinationMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionAddDestinationMessageCodec extends AbstractPacketCodec<SessionAddDestinationMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAddDestinationMessageCodec()
   {
      super(SESS_ADD_DESTINATION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   public int getBodyLength(final SessionAddDestinationMessage packet) throws Exception
   {
      return SimpleString.sizeofString(packet.getAddress()) + SIZE_BOOLEAN;
   }
   
   @Override
   protected void encodeBody(final SessionAddDestinationMessage message, final RemotingBuffer out) throws Exception
   {
      out.putSimpleString(message.getAddress());
      out.putBoolean(message.isTemporary());
   }

   @Override
   protected SessionAddDestinationMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      SimpleString address = in.getSimpleString();
      boolean temp = in.getBoolean();    
      return new SessionAddDestinationMessage(address, temp);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


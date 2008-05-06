/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_REMOVE_DESTINATION;
import static org.jboss.messaging.util.DataConstants.SIZE_BOOLEAN;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;
import org.jboss.messaging.util.DataConstants;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A SessionRemoveDestinationMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionRemoveDestinationMessageCodec extends AbstractPacketCodec<SessionRemoveDestinationMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionRemoveDestinationMessageCodec()
   {
      super(SESS_REMOVE_DESTINATION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   public int getBodyLength(final SessionRemoveDestinationMessage packet) throws Exception
   {   	
   	SimpleString address = packet.getAddress();      
      int bodyLength = SimpleString.sizeofString(address) + SIZE_BOOLEAN;
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionRemoveDestinationMessage message, final RemotingBuffer out) throws Exception
   {
      SimpleString address = message.getAddress();
     
      out.putSimpleString(address);
      out.putBoolean(message.isTemporary());
   }

   @Override
   protected SessionRemoveDestinationMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      SimpleString address = in.getSimpleString();
    
      return new SessionRemoveDestinationMessage(address, in.getBoolean());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

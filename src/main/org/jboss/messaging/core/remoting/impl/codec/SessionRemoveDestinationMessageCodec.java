/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_REMOVE_DESTINATION;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;

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

   protected int getBodyLength(final SessionRemoveDestinationMessage packet) throws Exception
   {   	
   	String address = packet.getAddress();      
      int bodyLength = sizeof(address) + BOOLEAN_LENGTH;
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionRemoveDestinationMessage message, final RemotingBuffer out) throws Exception
   {
      String address = message.getAddress();
     
      out.putNullableString(address);
      out.putBoolean(message.isTemporary());
   }

   @Override
   protected SessionRemoveDestinationMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      String address = in.getNullableString();
    
      return new SessionRemoveDestinationMessage(address, in.getBoolean());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

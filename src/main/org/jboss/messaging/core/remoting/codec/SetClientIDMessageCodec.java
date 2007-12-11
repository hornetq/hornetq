/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SETCLIENTID;

import org.jboss.messaging.core.remoting.wireformat.SetClientIDMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SetClientIDMessageCodec extends
      AbstractPacketCodec<SetClientIDMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SetClientIDMessageCodec()
   {
      super(MSG_SETCLIENTID);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SetClientIDMessage message, RemotingBuffer out) throws Exception
   {
      String clientID = message.getClientID();

      out.putInt(sizeof(clientID));
      out.putNullableString(clientID);
   }

   @Override
   protected SetClientIDMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String clientID = in.getNullableString();

      return new SetClientIDMessage(clientID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

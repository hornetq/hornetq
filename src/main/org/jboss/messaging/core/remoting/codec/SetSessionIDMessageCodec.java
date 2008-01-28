/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SETSESSIONID;

import org.jboss.messaging.core.remoting.wireformat.SetSessionIDMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class SetSessionIDMessageCodec extends
      AbstractPacketCodec<SetSessionIDMessage>
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SetSessionIDMessageCodec()
   {
      super(MSG_SETSESSIONID);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SetSessionIDMessage message, RemotingBuffer out)
         throws Exception
   {
      String sessionID = message.getSessionID();

      out.putInt(sizeof(sessionID));
      out.putNullableString(sessionID);
   }

   @Override
   protected SetSessionIDMessage decodeBody(RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (bodyLength > in.remaining())
      {
         return null;
      }
      String sessionID = in.getNullableString();

      return new SetSessionIDMessage(sessionID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
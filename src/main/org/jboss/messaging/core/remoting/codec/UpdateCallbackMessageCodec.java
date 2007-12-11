/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UPDATECALLBACK;

import org.jboss.messaging.core.remoting.wireformat.UpdateCallbackMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class UpdateCallbackMessageCodec extends
      AbstractPacketCodec<UpdateCallbackMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public UpdateCallbackMessageCodec()
   {
      super(MSG_UPDATECALLBACK);
   }

   // AbstractPackedCodec overrides----------------------------------

   @Override
   protected void encodeBody(UpdateCallbackMessage message, RemotingBuffer out) throws Exception
   {
      String remotingSessionID = message.getRemotingSessionID();
      String clientVMID = message.getClientVMID();
      boolean add = message.isAdd();

      int bodyLength = sizeof(remotingSessionID) + sizeof(clientVMID) + 1;

      out.putInt(bodyLength);
      out.putNullableString(remotingSessionID);
      out.putNullableString(clientVMID);
      out.putBoolean(add);
   }

   @Override
   protected UpdateCallbackMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      String remotingSessionID = in.getNullableString();
      String clientVMID = in.getNullableString();
      boolean add = in.getBoolean();

      return new UpdateCallbackMessage(remotingSessionID, clientVMID, add);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

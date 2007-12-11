/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONNECTION;

import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class ConnectionFactoryCreateConnectionRequestCodec extends
      AbstractPacketCodec<CreateConnectionRequest>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public ConnectionFactoryCreateConnectionRequestCodec()
   {
      super(REQ_CREATECONNECTION);
   }

   // AbstractPackedCodec overrides----------------------------------

   @Override
   protected void encodeBody(CreateConnectionRequest request,
         RemotingBuffer out)
         throws Exception
   {
      byte version = request.getVersion();
      String remotingSessionID = request.getRemotingSessionID();
      String clientVMID = request.getClientVMID();
      int failedNodeID = request.getFailedNodeID();
      String username = request.getUsername();
      String password = request.getPassword();

      int bodyLength = 1 // version
            + sizeof(remotingSessionID)
            + sizeof(clientVMID)
            + INT_LENGTH // failedNodeID
            + sizeof(username) 
            + sizeof(password);

      out.putInt(bodyLength);
      out.put(version);
      out.putNullableString(remotingSessionID);
      out.putNullableString(clientVMID);
      out.putInt(failedNodeID);
      out.putNullableString(username);
      out.putNullableString(password);
   }

   @Override
   protected CreateConnectionRequest decodeBody(
         RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }
      byte version = in.get();
      String remotingSessionID = in.getNullableString();
      String clientVMID = in.getNullableString();
      int failedNodeID = in.getInt();
      String username = in.getNullableString();
      String password = in.getNullableString();

      return new CreateConnectionRequest(version, remotingSessionID, 
            clientVMID, failedNodeID, username, password);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

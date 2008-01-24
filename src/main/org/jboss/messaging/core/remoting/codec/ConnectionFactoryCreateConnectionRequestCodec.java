/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONNECTION;

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
      String username = request.getUsername();
      String password = request.getPassword();
      int prefetchSize = request.getPrefetchSize();
      int dupOkBatchSize = request.getDupsOKBatchSize();
      String clientID = request.getClientID();

      int bodyLength = 1 // version
            + sizeof(remotingSessionID)
            + sizeof(clientVMID)
            + sizeof(username) 
            + sizeof(password)
            + INT_LENGTH
            + INT_LENGTH
            + sizeof(clientID);

      out.putInt(bodyLength);
      out.put(version);
      out.putNullableString(remotingSessionID);
      out.putNullableString(clientVMID);
      out.putNullableString(username);
      out.putNullableString(password);
      out.putInt(prefetchSize);
      out.putInt(dupOkBatchSize);
      out.putNullableString(clientID);
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
      String username = in.getNullableString();
      String password = in.getNullableString();
      int prefetchSize = in.getInt();
      int dupOkBatchSize = in.getInt();
      String clientID = in.getNullableString();

      return new CreateConnectionRequest(version, remotingSessionID,
            clientVMID, username, password, prefetchSize, dupOkBatchSize, clientID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

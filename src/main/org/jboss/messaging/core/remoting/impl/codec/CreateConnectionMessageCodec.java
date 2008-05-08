/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CREATECONNECTION;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class CreateConnectionMessageCodec extends  AbstractPacketCodec<CreateConnectionRequest>
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(CreateConnectionMessageCodec.class);
   
   
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public CreateConnectionMessageCodec()
   {
      super(CREATECONNECTION);
   }

   // AbstractPackedCodec overrides----------------------------------

   @Override
   protected void encodeBody(final CreateConnectionRequest request, final MessagingBuffer out)
         throws Exception
   {
      int version = request.getVersion();
      long remotingSessionID = request.getRemotingSessionID();
      String username = request.getUsername();
      String password = request.getPassword();

      out.putInt(version);
      out.putLong(remotingSessionID);
      out.putNullableString(username);
      out.putNullableString(password);
   }

   @Override
   protected CreateConnectionRequest decodeBody(final MessagingBuffer in) throws Exception
   {
      int version = in.getInt();
      long remotingSessionID = in.getLong();
      String username = in.getNullableString();
      String password = in.getNullableString();

      return new CreateConnectionRequest(version, remotingSessionID, username, password);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

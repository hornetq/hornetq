/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CREATECONNECTION_RESP;

import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.version.impl.VersionImpl;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.DataConstants;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class CreateConnectionResponseMessageCodec extends AbstractPacketCodec<CreateConnectionResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public CreateConnectionResponseMessageCodec()
   {
      super(CREATECONNECTION_RESP);
   }

   // AbstractPackedCodec overrides----------------------------------

   public int getBodyLength(final CreateConnectionResponse packet) throws Exception
   {
      return DataConstants.SIZE_LONG +
              sizeof(packet.getServerVersion().getVersionName()) +
              4 * DataConstants.SIZE_INT +
              sizeof(packet.getServerVersion().getVersionSuffix());
   }

   @Override
   protected void encodeBody(final CreateConnectionResponse response, final RemotingBuffer out)
           throws Exception
   {
      out.putLong(response.getConnectionTargetID());
      out.putNullableString(response.getServerVersion().getVersionName());
      out.putInt(response.getServerVersion().getMajorVersion());
      out.putInt(response.getServerVersion().getMinorVersion());
      out.putInt(response.getServerVersion().getMicroVersion());
      out.putInt(response.getServerVersion().getIncrementingVersion());
      out.putNullableString(response.getServerVersion().getVersionSuffix());
   }

   @Override
   protected CreateConnectionResponse decodeBody(final RemotingBuffer in) throws Exception
   {
      long connectionTargetID = in.getLong();
      String versionName = in.getNullableString();
      int majorVersion = in.getInt();
      int minorVersion = in.getInt();
      int microVersion = in.getInt();
      int incrementingVersion = in.getInt();
      String versionSuffix = in.getNullableString();
      Version version =  new VersionImpl(versionName, majorVersion, minorVersion, microVersion, incrementingVersion, versionSuffix);
      return new CreateConnectionResponse(connectionTargetID, version);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

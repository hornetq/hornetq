/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATEBROWSER;

import org.jboss.messaging.core.remoting.wireformat.CreateBrowserResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class CreateBrowserResponseCodec extends
      AbstractPacketCodec<CreateBrowserResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateBrowserResponseCodec()
   {
      super(RESP_CREATEBROWSER);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(CreateBrowserResponse response,
         RemotingBuffer out) throws Exception
   {
      String browserID = response.getBrowserID();

      int bodyLength = sizeof(browserID);
      
      out.putInt(bodyLength);
      out.putNullableString(browserID);
   }

   @Override
   protected CreateBrowserResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String browserID = in.getNullableString();

      return new CreateBrowserResponse(browserID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

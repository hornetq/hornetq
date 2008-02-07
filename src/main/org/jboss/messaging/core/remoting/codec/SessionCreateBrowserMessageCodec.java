/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_CREATEBROWSER;

import org.jboss.messaging.core.remoting.wireformat.SessionCreateBrowserMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class SessionCreateBrowserMessageCodec extends
      AbstractPacketCodec<SessionCreateBrowserMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateBrowserMessageCodec()
   {
      super(SESS_CREATEBROWSER);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionCreateBrowserMessage request, RemotingBuffer out) throws Exception
   {
      String queueName = request.getQueueName();
      String filterString = request.getFilterString();

      int bodyLength = sizeof(queueName) + sizeof(filterString);

      out.putInt(bodyLength);
      out.putNullableString(queueName);
      out.putNullableString(filterString);
   }

   @Override
   protected SessionCreateBrowserMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String queueName = in.getNullableString();
      String filterString = in.getNullableString();

      return new SessionCreateBrowserMessage(queueName, filterString);
   }

   // Inner classes -------------------------------------------------
}

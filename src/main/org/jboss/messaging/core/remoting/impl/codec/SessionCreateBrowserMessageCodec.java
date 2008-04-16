/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEBROWSER;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateBrowserMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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

   public int getBodyLength(final SessionCreateBrowserMessage packet) throws Exception
   {   	
   	String queueName = packet.getQueueName();
      String filterString = packet.getFilterString();

      int bodyLength = sizeof(queueName) + sizeof(filterString);
      
      return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionCreateBrowserMessage request, final RemotingBuffer out) throws Exception
   {
      String queueName = request.getQueueName();
      String filterString = request.getFilterString();

      out.putNullableString(queueName);
      out.putNullableString(filterString);
   }

   @Override
   protected SessionCreateBrowserMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      String queueName = in.getNullableString();
      String filterString = in.getNullableString();

      return new SessionCreateBrowserMessage(queueName, filterString);
   }

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BROWSER_HASNEXTMESSAGE_RESP;
import static org.jboss.messaging.util.DataConstants.SIZE_BOOLEAN;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionBrowserHasNextMessageResponseMessage;
import org.jboss.messaging.util.DataConstants;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SessionBrowserHasNextMessageResponseMessageCodec extends
      AbstractPacketCodec<SessionBrowserHasNextMessageResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionBrowserHasNextMessageResponseMessageCodec()
   {
      super(SESS_BROWSER_HASNEXTMESSAGE_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   public int getBodyLength(final SessionBrowserHasNextMessageResponseMessage packet) throws Exception
   {   	
      return SIZE_BOOLEAN;
   }
   
   @Override
   protected void encodeBody(final SessionBrowserHasNextMessageResponseMessage response,
         final RemotingBuffer out) throws Exception
   {
      out.putBoolean(response.hasNext());
   }

   @Override
   protected SessionBrowserHasNextMessageResponseMessage decodeBody(final RemotingBuffer in) throws Exception
   {
      boolean hasNext = in.getBoolean();

      return new SessionBrowserHasNextMessageResponseMessage(hasNext);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

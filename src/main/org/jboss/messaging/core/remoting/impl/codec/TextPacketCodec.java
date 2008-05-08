/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.TEXT;

import org.jboss.messaging.core.remoting.impl.wireformat.TextPacket;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class TextPacketCodec extends AbstractPacketCodec<TextPacket>
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public TextPacketCodec()
   {
      super(TEXT);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------
 
   @Override
   protected void encodeBody(TextPacket packet, MessagingBuffer out)
         throws Exception
   {
      String text = packet.getText();
      out.putNullableString(text);
   }

   @Override
   protected TextPacket decodeBody(MessagingBuffer in)
         throws Exception
   {
      String text = in.getNullableString();

      
      
      return new TextPacket(text);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

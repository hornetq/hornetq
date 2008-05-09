/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class TextPacket extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String text;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public TextPacket(final String text)
   {
      super(TEXT);

      this.text = text;
   }
   
   public TextPacket()
   {
      super(TEXT);
   }
   
   // Public --------------------------------------------------------

   public String getText()
   {
      return text;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putNullableString(text);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      text = buffer.getNullableString();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", text=" + text + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------   
}

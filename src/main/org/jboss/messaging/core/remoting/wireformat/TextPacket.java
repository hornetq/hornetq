/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.TEXT;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class TextPacket extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String text;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public TextPacket(String text)
   {
      super(TEXT);

      assert text != null;

      this.text = text;
   }
   
   // Public --------------------------------------------------------

   public String getText()
   {
      return text;
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

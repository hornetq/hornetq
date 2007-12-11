/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_BROWSER_NEXTMESSAGEBLOCK;

import java.util.Arrays;

import org.jboss.jms.message.JBossMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class BrowserNextMessageBlockResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JBossMessage[] messages;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BrowserNextMessageBlockResponse(JBossMessage[] messages)
   {
      super(RESP_BROWSER_NEXTMESSAGEBLOCK);

      assert messages != null;

      this.messages = messages;
   }

   // Public --------------------------------------------------------

   public JBossMessage[] getMessages()
   {
      return messages;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", messages=" + Arrays.asList(messages) + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

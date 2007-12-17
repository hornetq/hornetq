/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDMESSAGE;

import org.jboss.messaging.newcore.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SendMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Message message;
   private final boolean checkForDuplicates;
   private final long sequence;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SendMessage(Message message, boolean checkForDuplicates,
         long sequence)
   {
      super(MSG_SENDMESSAGE);

      assert message != null;

      this.message = message;
      this.checkForDuplicates = checkForDuplicates;
      this.sequence = sequence;
   }

   // Public --------------------------------------------------------

   public Message getMessage()
   {
      return message;
   }

   public boolean checkForDuplicates()
   {
      return checkForDuplicates;
   }

   public long getSequence()
   {
      return sequence;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", message=" + message
            + ", checkForDuplicates=" + checkForDuplicates + ", sequence="
            + sequence + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

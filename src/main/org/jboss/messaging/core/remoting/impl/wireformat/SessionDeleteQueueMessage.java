/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_DELETE_QUEUE;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 * @version <tt>$Revision$</tt>
 */
public class SessionDeleteQueueMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String queueName;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionDeleteQueueMessage(final String queueName)
   {
      super(SESS_DELETE_QUEUE);

      this.queueName = queueName;
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append("]");
      return buff.toString();
   }
   
   public String getQueueName()
   {
      return queueName;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

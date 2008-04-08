/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEQUEUE;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 * @version <tt>$Revision$</tt>
 */
public class SessionCreateQueueMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String address;
   private final String queueName;
   private final String filterString;
   private final boolean durable;
   private final boolean temporary;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateQueueMessage(final String address, final String queueName,
   		final String filterString, final boolean durable, final boolean temporary)
   {
      super(SESS_CREATEQUEUE);

      this.address = address;
      this.queueName = queueName;
      this.filterString = filterString;
      this.durable = durable;
      this.temporary = temporary;
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", address=" + address);
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append(", durable=" + durable);
      buff.append(", temporary=" + temporary);
      buff.append("]");
      return buff.toString();
   }
   
   public String getAddress()
   {
      return address;
   }

   public String getQueueName()
   {
      return queueName;
   }

   public String getFilterString()
   {
      return filterString;
   }

   public boolean isDurable()
   {
      return durable;
   }
   
   public boolean isTemporary()
   {
      return temporary;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

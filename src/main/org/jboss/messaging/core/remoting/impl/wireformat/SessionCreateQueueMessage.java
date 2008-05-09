/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 * @version <tt>$Revision$</tt>
 */
public class SessionCreateQueueMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString address;
   private SimpleString queueName;
   private SimpleString filterString;
   private boolean durable;
   private boolean temporary;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateQueueMessage(final SimpleString address, final SimpleString queueName,
   		final SimpleString filterString, final boolean durable, final boolean temporary)
   {
      super(SESS_CREATEQUEUE);

      this.address = address;
      this.queueName = queueName;
      this.filterString = filterString;
      this.durable = durable;
      this.temporary = temporary;
   }
   
   public SessionCreateQueueMessage()
   {
      super(SESS_CREATEQUEUE);
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
   
   public SimpleString getAddress()
   {
      return address;
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public SimpleString getFilterString()
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
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putSimpleString(address);
      buffer.putSimpleString(queueName);
      buffer.putNullableSimpleString(filterString);
      buffer.putBoolean(durable);
      buffer.putBoolean(temporary);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      address = buffer.getSimpleString();
      queueName = buffer.getSimpleString();
      filterString = buffer.getNullableSimpleString();
      durable = buffer.getBoolean();
      temporary = buffer.getBoolean();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

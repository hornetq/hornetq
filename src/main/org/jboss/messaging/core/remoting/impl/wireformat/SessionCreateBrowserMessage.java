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
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SessionCreateBrowserMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString queueName;
   
   private SimpleString filterString;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateBrowserMessage(final SimpleString queueName, final SimpleString filterString)
   {
      super(SESS_CREATEBROWSER);

      this.queueName = queueName;
      this.filterString = filterString;
   }
   
   public SessionCreateBrowserMessage()
   {
      super(SESS_CREATEBROWSER);
   }

   // Public --------------------------------------------------------

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putSimpleString(queueName);
      buffer.putNullableSimpleString(filterString);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      queueName = buffer.getSimpleString();
      filterString = buffer.getNullableSimpleString();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", queueName=" + queueName + ", filterString="
            + filterString + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEBROWSER;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SessionCreateBrowserMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String queueName;
   
   private final String filterString;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateBrowserMessage(final String queueName, final String filterString)
   {
      super(SESS_CREATEBROWSER);

      assert queueName != null;

      this.queueName = queueName;
      this.filterString = filterString;
   }

   // Public --------------------------------------------------------

   public String getQueueName()
   {
      return queueName;
   }

   public String getFilterString()
   {
      return filterString;
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

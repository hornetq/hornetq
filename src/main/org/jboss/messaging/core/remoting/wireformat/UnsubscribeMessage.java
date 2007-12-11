/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UNSUBSCRIBE;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class UnsubscribeMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String subscriptionName;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public UnsubscribeMessage(String subscriptionName)
   {
      super(MSG_UNSUBSCRIBE);

      assert subscriptionName != null;

      this.subscriptionName = subscriptionName;
   }

   // Public --------------------------------------------------------

   public String getSubscriptionName()
   {
      return subscriptionName;
   }
   
   @Override
   public String toString()
   {
      return getParentString() + ", subscriptionName=" + subscriptionName + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

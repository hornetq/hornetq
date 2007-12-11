/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETPREPAREDTRANSACTIONS;

import java.util.Arrays;

import org.jboss.jms.tx.MessagingXid;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class GetPreparedTransactionsResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final MessagingXid[] xids;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public GetPreparedTransactionsResponse(MessagingXid[] xids)
   {
    super(RESP_GETPREPAREDTRANSACTIONS);
    
    assert xids != null;
    
    this.xids = xids;
   }

   // Public --------------------------------------------------------

   public MessagingXid[] getXids()
   {
      return xids;
   }
   
   @Override
   public String toString()
   {
      return getParentString() + ", xids=" + Arrays.asList(xids) + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

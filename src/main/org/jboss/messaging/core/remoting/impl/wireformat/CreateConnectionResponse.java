/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CREATECONNECTION_RESP;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateConnectionResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String connectionID;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConnectionResponse(final String connectionID)
   {
      super(CREATECONNECTION_RESP);

      assertValidID(connectionID);

      this.connectionID = connectionID;
   }

   // Public --------------------------------------------------------

   public String getConnectionID()
   {
      return connectionID;
   }
   
   @Override
   public String toString()
   {
      return getParentString() + ", connectionID" + connectionID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

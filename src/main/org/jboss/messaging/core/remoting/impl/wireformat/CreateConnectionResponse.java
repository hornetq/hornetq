/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CREATECONNECTION_RESP;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateConnectionResponse extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long connectionTargetID;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConnectionResponse(final long connectionTargetID)
   {
      super(CREATECONNECTION_RESP);

      this.connectionTargetID = connectionTargetID;
   }

   // Public --------------------------------------------------------

   public long getConnectionTargetID()
   {
      return connectionTargetID;
   }
   
   @Override
   public String toString()
   {
      return getParentString() + ", connectionID" + connectionTargetID + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

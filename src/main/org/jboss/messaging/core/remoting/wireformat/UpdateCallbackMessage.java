/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UPDATECALLBACK;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class UpdateCallbackMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String remotingSessionID;

   private final String clientVMID;

   private final boolean add;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public UpdateCallbackMessage(String remotingSessionID, String clientVMID,
         boolean add)
   {
      super(MSG_UPDATECALLBACK);

      assertValidID(remotingSessionID);
      assertValidID(clientVMID);

      this.remotingSessionID = remotingSessionID;
      this.clientVMID = clientVMID;
      this.add = add;
   }

   // Public --------------------------------------------------------

   public String getRemotingSessionID()
   {
      return remotingSessionID;
   }

   public String getClientVMID()
   {
      return clientVMID;
   }

   /**
    * @return the add
    */
   public boolean isAdd()
   {
      return add;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", remotingSessionID=" + remotingSessionID
            + ", clientVMID=" + clientVMID + ", add=" + add + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateSessionResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String sessionID;

   private final int dupsOKBatchSize;

   private final boolean strictTCK;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateSessionResponse(String sessionID, int dupsOKBatchSize,
         boolean strictTCK)
   {
      super(PacketType.RESP_CREATESESSION);

      assertValidID(sessionID);

      this.sessionID = sessionID;
      this.dupsOKBatchSize = dupsOKBatchSize;
      this.strictTCK = strictTCK;
   }

   // Public --------------------------------------------------------

   public String getSessionID()
   {
      return sessionID;
   }

   public int getDupsOKBatchSize()
   {
      return dupsOKBatchSize;
   }

   public boolean isStrictTCK()
   {
      return strictTCK;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", sessionID=" + sessionID
            + ", dupsOKBatchSize=" + dupsOKBatchSize + ", strictTCK="
            + strictTCK + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

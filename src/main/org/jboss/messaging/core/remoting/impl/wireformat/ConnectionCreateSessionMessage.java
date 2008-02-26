/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONN_CREATESESSION;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ConnectionCreateSessionMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private final boolean xa;
   
   private final boolean autoCommitSends;
   
   private final boolean autoCommitAcks;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionCreateSessionMessage(final boolean xa, final boolean autoCommitSends, final boolean autoCommitAcks)
   {
      super(CONN_CREATESESSION);

      this.xa = xa;
      
      this.autoCommitSends = autoCommitSends;
      
      this.autoCommitAcks = autoCommitAcks;
   }

   // Public --------------------------------------------------------

   public boolean isXA()
   {
      return xa;
   }

   public boolean isAutoCommitSends()
   {
      return this.autoCommitSends;
   }
   
   public boolean isAutoCommitAcks()
   {
      return this.autoCommitAcks;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

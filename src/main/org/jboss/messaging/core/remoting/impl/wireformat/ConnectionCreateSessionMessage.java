/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ConnectionCreateSessionMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private boolean xa;
   
   private boolean autoCommitSends;
   
   private boolean autoCommitAcks;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionCreateSessionMessage(final boolean xa, final boolean autoCommitSends, final boolean autoCommitAcks)
   {
      super(CONN_CREATESESSION);

      this.xa = xa;
      
      this.autoCommitSends = autoCommitSends;
      
      this.autoCommitAcks = autoCommitAcks;
   }
   
   public ConnectionCreateSessionMessage()
   {
      super(CONN_CREATESESSION);
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
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putBoolean(xa);
      buffer.putBoolean(autoCommitSends);
      buffer.putBoolean(autoCommitAcks);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      xa = buffer.getBoolean();
      autoCommitSends = buffer.getBoolean();
      autoCommitAcks = buffer.getBoolean();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

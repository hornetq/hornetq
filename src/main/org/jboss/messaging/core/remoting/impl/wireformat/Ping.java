/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PING;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class Ping extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long sessionID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public Ping(final long sessionID)
   {
      super(PING);
      
      this.sessionID = sessionID;
   }
   
   // Public --------------------------------------------------------
   
   public long getSessionID()
   {
      return sessionID;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", sessionID=" + sessionID + "]";
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import org.apache.mina.common.IoSession;
import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.Packet;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaSession implements NIOSession
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final IoSession session;

   //private AtomicLong correlationCounter;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaSession(IoSession session)
   {
      assert session != null;

      this.session = session;
     // correlationCounter = new AtomicLong(0);
   }

   // Public --------------------------------------------------------

   public long getID()
   {
      return session.getId();
   }

   public void write(Packet packet)
   {
      session.write(packet);
   }

   public boolean isConnected()
   {
      return session.isConnected();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

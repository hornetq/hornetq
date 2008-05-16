/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import org.apache.mina.common.IoSession;
import org.jboss.messaging.core.logging.Logger;
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

   private static final Logger log = Logger.getLogger(MinaConnector.class);
   
   
   // Attributes ----------------------------------------------------

   private final IoSession session;

   private MinaHandler handler;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaSession(IoSession session, MinaHandler handler)
   {
      assert session != null;

      this.session = session;
  
      this.handler = handler;
   }

   // Public --------------------------------------------------------

   public long getID()
   {
      return session.getId();
   }
   
   
   public void write(Packet packet)
   {     
      try
      {
         handler.checkWrite(session);
      }
      catch (Exception e)
      {
         log.error("Failed to acquire sem", e);
      }
      
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

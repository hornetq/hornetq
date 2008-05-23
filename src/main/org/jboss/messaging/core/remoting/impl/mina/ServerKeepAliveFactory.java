/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;
import org.jboss.messaging.core.remoting.impl.wireformat.Pong;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ServerKeepAliveFactory implements KeepAliveFactory
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger
         .getLogger(ServerKeepAliveFactory.class);

   // Attributes ----------------------------------------------------

   // FIXME session mapping must be cleaned when the server session is closed:
   // either normally or exceptionally
   /**
    * Key = server session ID Value = client session ID
    */
   private List<Long> sessions = new ArrayList<Long>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // KeepAliveFactory implementation -------------------------------

   public Ping ping(long sessionID)
   {
      return new Ping(sessionID);
   }

   public boolean isPinging(long sessionID)
   {
      return sessions.contains(sessionID);
   }

   public Pong pong(long sessionID, Ping ping)
   {
      long clientSessionID = ping.getSessionID();
      return new Pong(sessionID, sessions.contains(clientSessionID));
   }

   public List<Long> getSessions()
   {
      return sessions;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

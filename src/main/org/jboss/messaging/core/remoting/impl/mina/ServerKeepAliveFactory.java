/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import java.util.Map;
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
   private Map<String, String> sessions = new ConcurrentHashMap<String, String>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // KeepAliveFactory implementation -------------------------------

   public Ping ping(String sessionID)
   {
      return new Ping(sessionID);
   }

   public boolean isPing(String sessionID, Object message)
   {
      if (!(message instanceof Ping))
      {
         return false;
      } else
      {
         Ping ping = (Ping) message;
         String clientSessionID = ping.getSessionID();
         if (clientSessionID.equals(sessionID))
         {
            return false;
         } else
         {
            if (log.isDebugEnabled())
               log.debug("associated server session " + sessionID
                     + " to client " + clientSessionID);
            sessions.put(sessionID, clientSessionID);
            return true;
         }
      }
   }

   public Pong pong(String sessionID, Ping ping)
   {
      String clientSessionID = ping.getSessionID();
      return new Pong(sessionID, sessions.containsKey(clientSessionID));
   }

   public Map<String, String> getSessions()
   {
      return sessions;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

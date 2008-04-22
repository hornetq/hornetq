/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import org.apache.mina.common.IoSession;
import org.apache.mina.filter.keepalive.KeepAliveMessageFactory;
import org.jboss.messaging.core.exception.MessagingException;
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
public class MinaKeepAliveFactory implements KeepAliveMessageFactory
{
   // Constant ------------------------------------------------------

   private static final Logger log = Logger
         .getLogger(MinaKeepAliveFactory.class);

   // Attributes ----------------------------------------------------

   private KeepAliveFactory innerFactory;

   private CleanUpNotifier notifier;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaKeepAliveFactory(KeepAliveFactory factory,
         CleanUpNotifier notifier)
   {
      assert factory != null;

      this.notifier = notifier;
      this.innerFactory = factory;
   }

   // Public --------------------------------------------------------

   // KeepAliveMessageFactory implementation ------------------------

   public Object getRequest(IoSession session)
   {
      return innerFactory.ping(session.getId());
   }

   public Object getResponse(IoSession session, Object request)
   {
      return innerFactory.pong(session.getId(), (Ping) request);
   }

   public boolean isRequest(IoSession session, Object request)
   {
      return innerFactory.isPing(session.getId(), request);
   }

   public boolean isResponse(IoSession session, Object response)
   {
      if (response instanceof Pong)
      {
         Pong pong = (Pong) response;
         if (pong.isSessionFailed() && notifier != null)
         {
            notifier.fireCleanup(session.getId(), new MessagingException(
                  MessagingException.CONNECTION_TIMEDOUT,
                  "Session has failed on the server"));
         }
         return true;
      } else
      {
         return false;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

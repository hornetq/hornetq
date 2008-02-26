/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import org.apache.mina.common.IoSession;
import org.apache.mina.filter.keepalive.KeepAliveMessageFactory;
import org.jboss.messaging.core.MessagingException;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.RemotingException;
import org.jboss.messaging.core.remoting.wireformat.Ping;
import org.jboss.messaging.core.remoting.wireformat.Pong;
import org.jboss.messaging.util.Logger;

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

   private FailureNotifier notifier;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaKeepAliveFactory(KeepAliveFactory factory,
         FailureNotifier notifier)
   {
      assert factory != null;

      this.notifier = notifier;
      this.innerFactory = factory;
   }

   // Public --------------------------------------------------------

   // KeepAliveMessageFactory implementation ------------------------

   public Object getRequest(IoSession session)
   {
      return innerFactory.ping(Long.toString(session.getId()));
   }

   public Object getResponse(IoSession session, Object request)
   {
      assert request instanceof Ping;

      return innerFactory.pong(Long.toString(session.getId()), (Ping) request);
   }

   public boolean isRequest(IoSession session, Object request)
   {
      return innerFactory.isPing(Long.toString(session.getId()), request);
   }

   public boolean isResponse(IoSession session, Object response)
   {
      if (response instanceof Pong)
      {
         Pong pong = (Pong) response;
         if (pong.isSessionFailed() && notifier != null)
         {
            // FIXME better error code
            notifier.fireFailure(new RemotingException(
                  MessagingException.CONNECTION_TIMEDOUT,
                  "Session has failed on the server", Long.toString(session
                        .getId())));
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

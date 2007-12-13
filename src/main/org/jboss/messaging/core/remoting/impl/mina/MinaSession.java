/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import java.util.concurrent.TimeUnit;

import org.apache.mina.common.IoSession;
import org.apache.mina.filter.reqres.Request;
import org.apache.mina.filter.reqres.Response;
import org.jboss.messaging.core.remoting.NIOSession;

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

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaSession(IoSession session)
   {
      assert session != null;

      this.session = session;
   }

   // Public --------------------------------------------------------

   public long getID()
   {
      return session.getId();
   }

   public void write(Object object)
   {
      session.write(object);
   }

   public Object writeAndBlock(long requestID, Object object, long timeout,
         TimeUnit timeUnit) throws Throwable
   {
      Request req = new Request(requestID, object, timeout, timeUnit);
      session.write(req);
      Response response = req.awaitResponse();
      return response.getMessage();
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

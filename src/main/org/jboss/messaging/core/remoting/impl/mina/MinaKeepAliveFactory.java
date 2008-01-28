/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import org.apache.mina.common.IoSession;
import org.apache.mina.filter.keepalive.KeepAliveMessageFactory;
import org.jboss.messaging.core.remoting.KeepAliveFactory;
import org.jboss.messaging.core.remoting.wireformat.Ping;
import org.jboss.messaging.core.remoting.wireformat.Pong;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class MinaKeepAliveFactory implements KeepAliveMessageFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private KeepAliveFactory innerFactory;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaKeepAliveFactory(KeepAliveFactory factory)
   {
      assert factory != null;
      
      this.innerFactory = factory;
   }

   // Public --------------------------------------------------------

   // KeepAliveMessageFactory implementation ------------------------
   
   public Object getRequest(IoSession session)
   {
      return innerFactory.ping();
   }

   public Object getResponse(IoSession session, Object request)
   {
      return innerFactory.pong();
   }

   public boolean isRequest(IoSession session, Object request)
   {
      return (request instanceof Ping);
   }

   public boolean isResponse(IoSession session, Object response)
   {
      return (response instanceof Pong);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

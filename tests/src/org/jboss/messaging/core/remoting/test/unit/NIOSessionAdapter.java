/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.test.unit;

import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.remoting.NIOSession;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class NIOSessionAdapter implements NIOSession
{    
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // NIOSession implementation -------------------------------------
 
   public long getID()
   {
      return 0;
   }

   public boolean isConnected()
   {
      return false;
   }

   public void write(Object object)
   {
   }
   
   public Object writeAndBlock(long requestID, Object object, long timeout,
         TimeUnit timeUnit) throws Throwable
   {
      return null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

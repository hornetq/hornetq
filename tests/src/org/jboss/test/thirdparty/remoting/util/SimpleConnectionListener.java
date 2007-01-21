/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.thirdparty.remoting.util;

import org.jboss.remoting.ConnectionListener;
import org.jboss.remoting.Client;

import java.io.Serializable;

import EDU.oswego.cs.dl.util.concurrent.Channel;
import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

/**
 * It is Serializable so it can be used both on client and on server.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SimpleConnectionListener implements ConnectionListener, Serializable
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 5457454557215716L;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private transient Channel failures;

   // Constructors ---------------------------------------------------------------------------------

   // ConnectionListener implementation -----------------------------------------------------------

   public void handleConnectionException(Throwable throwable, Client client)
   {
      init();
      try
      {
         failures.put(throwable);
      }
      catch(InterruptedException e)
      {
         throw new RuntimeException("Failed to record failure", e);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public Throwable getNextFailure(long timeout) throws InterruptedException
   {
      init();
      return (Throwable)failures.poll(timeout);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private synchronized void init()
   {
      if (failures == null)
      {
         failures = new LinkedQueue();
      }
   }

   // Inner classes --------------------------------------------------------------------------------
}

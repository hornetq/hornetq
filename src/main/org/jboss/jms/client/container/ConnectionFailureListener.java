/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.remoting.ConnectionListener;
import org.jboss.remoting.Client;
import org.jboss.logging.Logger;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.FailoverEvent;
import org.jboss.jms.client.FailoverCommandCenter;
import org.jboss.jms.client.state.ConnectionState;

/**
 * The listener that detects a connection failure and initiates the failover process. Each physical
 * connection created under the supervision of ClusteredAspect has one of these.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFailureListener implements ConnectionListener
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ConnectionFailureListener.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private FailoverCommandCenter fcc;

   // Constructors ---------------------------------------------------------------------------------

   ConnectionFailureListener(FailoverCommandCenter fcc)
   {
      this.fcc = fcc;
   }

   // ConnectionListener implementation ------------------------------------------------------------

   public void handleConnectionException(Throwable throwable, Client client)
   {
      try
      {
         log.debug(this + " is being notified of connection failure: " + throwable);

         fcc.failureDetected(throwable, null);

      }
      catch (Throwable e)
      {
         log.error("Caught exception in handling failure", e);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ConnectionFailureListener[" + fcc + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}

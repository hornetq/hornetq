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
import org.jboss.jms.client.FailoverCommandCenter;
import org.jboss.jms.client.FailureDetector;
import org.jboss.jms.client.remoting.JMSRemotingConnection;

/**
 * The listener that detects a connection failure and initiates the failover process. Each physical
 * connection created under the supervision of ClusteredAspect has one of these.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFailureListener implements ConnectionListener, FailureDetector
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ConnectionFailureListener.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private FailoverCommandCenter fcc;

   // The remoting connection is needed here to validate that the failure wasn't captured after
   // failover was already called
   private JMSRemotingConnection remotingConnection;

   // Constructors ---------------------------------------------------------------------------------

   ConnectionFailureListener(FailoverCommandCenter fcc, JMSRemotingConnection remotingConnection)
   {
      this.fcc = fcc;
      this.remotingConnection = remotingConnection;
   }

   // ConnectionListener implementation ------------------------------------------------------------

   public void handleConnectionException(Throwable throwable, Client client)
   {
      try
      {
         log.debug(this + " is being notified of connection failure: " + throwable);

         fcc.failureDetected(throwable, this, remotingConnection);

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

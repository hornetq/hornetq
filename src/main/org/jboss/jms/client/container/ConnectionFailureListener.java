/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.jms.client.FailoverCommandCenter;
import org.jboss.jms.client.FailureDetector;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.logging.Logger;

/**
 * The listener that detects a connection failure and initiates the failover process. Each physical
 * connection created under the supervision of ClusteredAspect has one of these.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFailureListener implements FailureDetector
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

   /*
    * Returns true if failover handled the exception gracefully
    * Returns false if failover was unable to handle the exception and it should be passed
    * on to any JMS exception listener
    */
   public boolean handleConnectionException(Throwable throwable)
   {
      try
      {
         log.trace(this + " is being notified of connection failure: " + throwable);

         return fcc.failureDetected(throwable, this, remotingConnection);
      }
      catch (Throwable e)
      {
         log.error("Caught exception in handling failure", e);
         
         return false;
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

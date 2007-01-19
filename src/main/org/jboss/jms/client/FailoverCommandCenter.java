/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.logging.Logger;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.server.endpoint.CreateConnectionResult;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * The class in charge with performing the failover.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class FailoverCommandCenter
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(FailoverCommandCenter.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private ConnectionState state;

   private FailoverValve valve;

   private List failoverListeners;

   // Constructors ---------------------------------------------------------------------------------

   public FailoverCommandCenter(ConnectionState state)
   {
      this.state = state;
      failoverListeners = new ArrayList();
      valve = new FailoverValve(this);
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * Method called by failure detection components (FailoverValveInterceptors and
    * ConnectionListeners) when they have reasons to belive that a server failure occured.
    */
   public void failureDetected(Throwable reason, FailureDetector source,
                               JMSRemotingConnection remotingConnection)
      throws Exception
   {
      log.debug("failure detected by " + source);

      // generate a FAILURE_DETECTED event
      broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILURE_DETECTED, this));

      log.debug(this + " initiating client-side failover");

      CreateConnectionResult res = null;
      boolean failoverSuccessful = false;

      try
      {
         // block any other invocations ariving to any delegate from the hierarchy while we're
         // doing failover

         valve.close();

         if (remotingConnection.isFailed())
         {
            log.debug(this + " ignoring failure detection notification, as failover was " +
               "already performed on this connection");
            return;
         }
         remotingConnection.setFailed(true);

         // generate a FAILOVER_STARTED event. The event must be broadcasted AFTER valve closure,
         // to insure the client-side stack is in a deterministic state
         broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILOVER_STARTED, this));

         int failedNodeID = state.getServerID();
         ConnectionFactoryDelegate clusteredDelegate =
            state.getClusteredConnectionFactoryDelegate();

         // re-try creating the connection
         res = clusteredDelegate.
            createConnectionDelegate(state.getUsername(), state.getPassword(), failedNodeID);

         if (res == null)
         {
            // No failover attempt was detected on the server side; this might happen if the
            // client side network fails temporarily so the client connection breaks but the
            // server cluster is still up and running - in this case we don't perform failover.
            failoverSuccessful = false;
         }
         else
         {
            // recursively synchronize state
            ClientConnectionDelegate newDelegate = (ClientConnectionDelegate)res.getDelegate();
            state.getDelegate().synchronizeWith(newDelegate);
            failoverSuccessful = true;
         }
      }
      finally
      {
         // I have this secondary try/finally block, just because if broadcastFailoverEvent throws
         // any exceptions I don't want a dead lock on everybody waiting the valve to be opened.
         try
         {
            if (failoverSuccessful)
            {
               broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILOVER_COMPLETED, this));
            }
            else
            {
               broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILOVER_FAILED, this));
            }
         }
         finally
         {
            valve.open();
         }
      }
   }

   public void registerFailoverListener(FailoverListener listener)
   {
      synchronized(failoverListeners)
      {
         failoverListeners.add(listener);
      }
   }

   public boolean unregisterFailoverListener(FailoverListener listener)
   {
      synchronized(failoverListeners)
      {
         return failoverListeners.remove(listener);
      }
   }

   public FailoverValve getValve()
   {
      return valve;
   }

   public JMSRemotingConnection getRemotingConnection()
   {
      return state.getRemotingConnection();
   }

   public String toString()
   {
      return "FailoverCommandCenter[" + state + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   ConnectionState getConnectionState()
   {
      return state;
   }

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void broadcastFailoverEvent(FailoverEvent e)
   {
      if (trace) { log.trace(this + " broadcasting " + e); }

      List listenersCopy;

      synchronized(failoverListeners)
      {
         listenersCopy = new ArrayList(failoverListeners);
      }

      for(Iterator i = listenersCopy.iterator(); i.hasNext(); )
      {
         FailoverListener listener = (FailoverListener)i.next();

         try
         {
            listener.failoverEventOccured(e);
         }
         catch(Exception ex)
         {
            log.warn("Failover listener " + listener + " did not accept event", ex);
         }
      }
   }

   // Inner classes --------------------------------------------------------------------------------
}

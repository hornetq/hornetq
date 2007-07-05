/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.CreateConnectionResult;
import org.jboss.logging.Logger;

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

   private FailoverValve2 valve;

   private List failoverListeners;
   
   // Constructors ---------------------------------------------------------------------------------

   public FailoverCommandCenter(ConnectionState state)
   {
      this.state = state;
      failoverListeners = new ArrayList();
      
      valve = new FailoverValve2();
   }

   // Public ---------------------------------------------------------------------------------------
   
   public void setState(ConnectionState state)
   {
      this.state = state;
   }
   
   /**
    * Method called by failure detection components (FailoverValveInterceptors and
    * ConnectionListeners) when they have reasons to believe that a server failure occured.
    * 
    * Returns true if the failover command centre handled the exception gracefully and failover completed
    * or false if it didn't and failover did not occur
    */
   public boolean failureDetected(Throwable reason, FailureDetector source,
                                  JMSRemotingConnection remotingConnection)
      throws Exception
   {
      log.debug("failure detected by " + source);

      // generate a FAILURE_DETECTED event
      broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILURE_DETECTED, source));

      CreateConnectionResult res = null;
      
      boolean failoverSuccessful = false;
      
      boolean valveOpened = false;
      
      try
      {
         // block any other invocations ariving to any delegate from the hierarchy while we're
         // doing failover

         valve.close();
         
         synchronized(this)
         {
            // testing for failed connection and setting the failed flag need to be done in one
            // atomic operation, otherwise multiple threads can get to perform the client-side
            // failover concurrently
            if (remotingConnection.isFailed())
            {
               log.debug(this + " ignoring failure detection notification, as failover was " +
                  "already (or is in process of being) performed on this connection");
               
               failoverSuccessful = true;
               
               //Return true since failover already completed ok
               return true;
            }

            remotingConnection.setFailed();
         }
         
         // Note - failover doesn't occur until _after_ the above check - so the next comment
         // belongs here
         log.debug(this + " starting client-side failover");
         
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
            // Failover did not occur
            failoverSuccessful = false;
         }
         else
         {      
            // recursively synchronize state
            ClientConnectionDelegate newDelegate = (ClientConnectionDelegate)res.getDelegate();
            
            state.getDelegate().synchronizeWith(newDelegate);
                           
            valve.open();
            valveOpened = true;
            
            //Now start the connection - note! this can't be done while the valve is closed
            //or it will block itself
            
            // start the connection again on the serverEndpoint if necessary
            if (state.isStarted())
            {
               newDelegate.start();
            }
            
            failoverSuccessful = true;                        
         }
         
         return failoverSuccessful;
      }
      catch (Exception e)
      {
         log.error("Failover failed", e);

         throw e;
      }
      finally
      {
         if (!valveOpened)
         {
            valve.open();
         }

         if (failoverSuccessful)
         {
            log.debug(this + " completed successful failover");
            broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILOVER_COMPLETED, this));
         }
         else
         {
            log.debug(this + " aborted failover");
            ClientConnectionDelegate connDelegate = (ClientConnectionDelegate)state.getDelegate();
            connDelegate.closing();
            connDelegate.close();
            
            broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILOVER_FAILED, this));
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

   public FailoverValve2 getValve()
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

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
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
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

   // Attributes-----------------------------------------------------------------------------------

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
      log.debug("failure detected by " + source, reason);

      // generate a FAILURE_DETECTED event
      broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILURE_DETECTED, source));

      CreateConnectionResult res = null;
      
      boolean failoverSuccessful = false;
      
      boolean valveOpened = false;

      int failoverEvent = FailoverEvent.FAILOVER_COMPLETED;
      
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

               failoverEvent = FailoverEvent.FAILOVER_ALREADY_COMPLETED;
               
               //Return true since failover already completed ok
               return true;
            }

            remotingConnection.setFailed();
         }
         
         // Note - failover doesn't occur until _after_ the above check - so the next comment
         // belongs here
         log.info("JBoss Messaging server failure detected - waiting for failover to complete...");
         
         // generate a FAILOVER_STARTED event. The event must be broadcasted AFTER valve closure,
         // to insure the client-side stack is in a deterministic state
         broadcastFailoverEvent(new FailoverEvent(FailoverEvent.FAILOVER_STARTED, this));        
         
         int failedNodeID = state.getServerID();
                  
         ConnectionFactoryDelegate clusteredDelegate = state.getClusteredConnectionFactoryDelegate();
                           
         // try recreating the connection
         log.trace("Creating new connection");
         res = clusteredDelegate.
            createConnectionDelegate(state.getUsername(), state.getPassword(), failedNodeID);
         log.trace("Created connection");
         
         if (res == null)
         {
            // Failover did not occur
            failoverSuccessful = false;
            log.trace("No failover");
         }
         else
         {      
            // recursively synchronize state
            ClientConnectionDelegate newDelegate = (ClientConnectionDelegate)res.getDelegate();
            
            log.trace("Synchronizing state");
            state.getDelegate().synchronizeWith(newDelegate);
            log.trace("Synchronized state");
            
            //Now restart the connection if appropriate
            //Note! we mus start the connection while the valve is still closed
            //Otherwise If a consumer closing is waiting on failover to complete
            //Then on failover complete the valve will be opened and closing retried on a
            //different thread
            //but the next line will re-start the connection so there is a race between the two
            //If the restart hits after closing then messages can get delivered after consumer
            //is closed
            
            if (state.isStarted())
            {
               log.trace("Starting new connection");
               newDelegate.startAfterFailover();
               log.trace("Started new connection");
            }
                           
            log.trace("Opening valve");
            valve.open();
            log.trace("Opened valve");
            valveOpened = true;
            
            failoverSuccessful = true;      
            
            log.info("JBoss Messaging failover complete");
         }
         
         log.trace("failureDetected() complete");
         
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
         	log.trace("finally opening valve");
            valve.open();
            log.trace("valve opened");
         }

         if (failoverSuccessful)
         {
            log.debug(this + " completed successful failover");
            broadcastFailoverEvent(new FailoverEvent(failoverEvent, this));
         }
         else
         {
            log.debug(this + " aborted failover");
            ClientConnectionDelegate connDelegate = (ClientConnectionDelegate)state.getDelegate();
            connDelegate.closing(-1);
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

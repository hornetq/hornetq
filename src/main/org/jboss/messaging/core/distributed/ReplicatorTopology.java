/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.logging.Logger;
import org.jboss.messaging.util.RpcServerCall;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.util.ServerResponse;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Collections;
import java.util.Collection;

/**
 * The Observers interested in topology changes should register here.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReplicatorTopology
      extends Observable
      implements ReplicatorTopologyServerDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatorTopology.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   protected Replicator peer;

   protected Set topology;

   // Constructors --------------------------------------------------

   ReplicatorTopology(Replicator peer)
   {
      super();
      this.peer = peer;
      topology = new HashSet();
   }

   // ReplicatorTopologyServerDelegate implementation ---------------

   public Serializable getID()
   {
      return peer.getPeerID();
   }

   public void outputPeerJoins(Serializable joiningPeerID) throws Exception
   {
      log.debug(this + ".outputPeerJoins(" + joiningPeerID + ")");

      synchronized(topology)
      {
         if (topology.contains(joiningPeerID))
         {
            String msg = "Duplicate peer ID: " + joiningPeerID + ", rejected";
            log.warn(msg);
            throw new Exception(msg);
         }
         topology.add(joiningPeerID);
         setChanged();
         notifyObservers();
      }
   }

   public void outputPeerLeaves(Serializable leavingPeerID)
   {
      throw new NotYetImplementedException("Don't know to handle a leaving peer");
   }

   public Set getView()
   {
      Set result;
      synchronized(topology)
      {
         if (topology.isEmpty())
         {
            result = Collections.EMPTY_SET;
         }
         else
         {
            result = new HashSet();
            for(Iterator i = topology.iterator(); i.hasNext(); )
            {
               result.add(i.next());
            }
         }
      }
      log.debug(this + ".getView() returns " + result);
      return result;
   }


   // Public --------------------------------------------------------

   public void aquireInitialTopology(RpcDispatcher dispatcher) throws DistributedException
   {
      // I won't send this call on myself since at this time, my ReplicatorTopology is not
      // registered yet.
      RpcServerCall call =
            new RpcServerCall(peer.getReplicatorID(),
                              "getView",
                              new Object[] {},
                              new String[] {});
      // TODO - deal with the timeout
      Collection c = call.remoteInvoke(dispatcher, 30000);

      try
      {
         for(Iterator i = c.iterator(); i.hasNext(); )
         {
            Object o = i.next();
            if (o instanceof Throwable)
            {
               throw (Throwable)o;
            }
            Set view = (Set)((ServerResponse)o).getInvocationResult();
            if (!view.isEmpty())
            {
               synchronized(topology)
               {
                  topology.addAll(view);
               }
               setChanged();
               notifyObservers();
            }
            // used the first answer, exit now
            break;
         }
      }
      catch(Throwable t)
      {
         throw new DistributedException("Failed to acquire the intial topology", t);
      }
   }

   public void registerTopologyListener(Observer observer)
   {
      addObserver(observer);
   }

   /**
    * Frees up resources and detaches the observers.
    */
   public void stop()
   {
      topology.clear();
      deleteObservers();
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("Topology[");
      sb.append(peer.getPeerID());
      sb.append("]");
      return sb.toString();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

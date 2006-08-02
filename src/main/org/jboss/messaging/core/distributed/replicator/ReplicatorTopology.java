/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.messaging.core.distributed.replicator;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jgroups.Address;
import org.jgroups.blocks.RpcDispatcher;

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

   /** <outputPeerID - output peer JGroups address> */
   protected Map topology;

   // Constructors --------------------------------------------------

   ReplicatorTopology(Replicator peer)
   {
      super();
      this.peer = peer;
      topology = new HashMap();
   }

   // ReplicatorTopologyServerDelegate implementation ---------------

   public Serializable getID()
   {
//      return peer.getPeerID();
      throw new NotYetImplementedException();
   }

   public void outputPeerJoins(Serializable joiningPeerID, Address address) throws Exception
   {
      log.debug(this + ".outputPeerJoins(" + joiningPeerID + ", " + address + ")");

      synchronized(topology)
      {
         if (topology.containsKey(joiningPeerID))
         {
            String msg = "Duplicate peer ID: " + joiningPeerID + ", rejected";
            log.warn(msg);
            throw new Exception(msg);
         }
         topology.put(joiningPeerID, address);
         setChanged();
         notifyObservers();
      }
   }

   public void outputPeerLeaves(Serializable leavingPeerID)
   {
      throw new NotYetImplementedException("Don't know to handle a leaving peer");
   }

   /**
    * @return a set of output peer IDs.
    */
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
            for(Iterator i = topology.keySet().iterator(); i.hasNext(); )
            {
               result.add(i.next());
            }
         }
      }
      log.debug(this + ".getView() returns " + result);
      return result;
   }

   public Map getViewMap()
   {
      Map result;
      synchronized(topology)
      {
         if (topology.isEmpty())
         {
            result = Collections.EMPTY_MAP;
         }
         else
         {
            result = new HashMap();
            for(Iterator i = topology.keySet().iterator(); i.hasNext(); )
            {
               Object o = i.next();
               // TODO do I need to make a clone of the Address?
               result.put(o, topology.get(o));
            }
         }
      }
      log.debug(this + ".getViewMap() returns " + result);
      return result;
   }



   // Public --------------------------------------------------------

   public Address getAddress(Serializable outputPeerID)
   {
      synchronized(topology)
      {
         return (Address)topology.get(outputPeerID);
      }
   }

   public void aquireInitialTopology(RpcDispatcher dispatcher) throws DistributedException
   {
//      // Only output peers register IdentityDelegates
//      RpcServerCall call = new RpcServerCall(peer.getReplicatorID(),
//                                             "getIdentity",
//                                             new Object[] {},
//                                             new String[] {});
//      // TODO - deal with the timeout
//      Collection c = call.remoteInvoke(dispatcher, 30000);
//
//      try
//      {
//         for(Iterator i = c.iterator(); i.hasNext(); )
//         {
//            Object o = i.next();
//            if (o instanceof Throwable)
//            {
//               throw (Throwable)o;
//            }
//            o = ((ServerResponse)o).getInvocationResult();
//            if (o instanceof NoSuchMethodException)
//            {
//               // just ignore it, it means that I reached a input peer that doesn't answer
//               // identity calls
//               continue;
//            }
//            PeerIdentity outputPeerIdentity = (PeerIdentity)o;
//            synchronized(topology)
//            {
//               topology.put(outputPeerIdentity.getPeerID(), outputPeerIdentity.getAddress());
//            }
//            setChanged();
//            notifyObservers();
//         }
//      }
//      catch(Throwable t)
//      {
//         throw new DistributedException("Failed to acquire the intial topology", t);
//      }
//
//      log.debug("Initial topology: " + topology);
      throw new NotYetImplementedException();
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
//      StringBuffer sb = new StringBuffer("Topology[");
//      sb.append(peer.getPeerID());
//      sb.append("]");
//      return sb.toString();
      throw new NotYetImplementedException();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

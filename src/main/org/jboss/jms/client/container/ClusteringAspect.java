/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.logging.Logger;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.FailoverCommandCenter;
import org.jboss.jms.server.endpoint.CreateConnectionResult;

import javax.jms.JMSException;
import java.util.Map;

/**
 * This aspect is part of a clustered ConnectionFactory aspect stack.
 * Some of its responsibilities are:
 *
 * - To choose the next node to create a physical connection to, based on a pluggable load balancing
 *   policy.
 * - To handle physical connection creation (by delegating it to a non-clustered ConnectionFactory
 *   delegate) and instal failure listeners.
 * - etc.
 *
 * It's a PER_INSTANCE aspect (one of these per each clustered ConnectionFactory instance)
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClusteringAspect
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClusteringAspect.class);

   public static final int MAX_RECONNECT_HOP_COUNT = 10;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // This is a PER_INSTANCE aspect, so it has a 1-to-1 relationship with its delegate
   private ClientClusteredConnectionFactoryDelegate clusteredDelegate;

   // The index of the next non-clustered delegate to be used. Only access it from a synchronized
   // block. Currently hardcoded round-robin, the algorithm must be made pluggable.
   private int next;

   // Constructors ---------------------------------------------------------------------------------

   public ClusteringAspect()
   {
      next = 0;
   }

   // Public ---------------------------------------------------------------------------------------

   public CreateConnectionResult handleCreateConnectionDelegate(Invocation invocation)
      throws Throwable
   {
      // initalize this PER_INSTANCE aspect by getting a hold of its corresponding clustered
      // delegate and maintaining a reference to it
      if (clusteredDelegate == null)
      {
         clusteredDelegate = (ClientClusteredConnectionFactoryDelegate)invocation.getTargetObject();
      }

      // the method handles both the case of a first connection creation attempt and a re-try during
      // a client-side failover. The difference is given by the failedNodeID (-1 for first attempt)

      MethodInvocation mi = (MethodInvocation)invocation;
      String username = (String)mi.getArguments()[0];
      String password = (String)mi.getArguments()[1];
      Integer failedNodeID = (Integer)mi.getArguments()[2];

      // We attempt to connect to the next node in a loop, since we might need to go through
      // multiple hops

      int attemptCount = 0;
      ClientConnectionFactoryDelegate delegate = null;

      while (attemptCount < MAX_RECONNECT_HOP_COUNT)
      {

         int failedNodeIDToServer = -1;

         if (delegate == null)
         {
            if (failedNodeID != null && failedNodeID.intValue() >= 0)
            {
               delegate = getFailoverDelegateForNode(failedNodeID);
               failedNodeIDToServer = failedNodeID.intValue();
            }
            else
            {
               delegate = getNextRoundRobinDelegate();
            }
         }

         log.debug(this + " has chosen " + delegate + " as target, " +
            (attemptCount == 0 ? "first connection attempt" : attemptCount + " connection attempts"));

         CreateConnectionResult res = delegate.
            createConnectionDelegate(username, password, failedNodeIDToServer);
         ClientConnectionDelegate cd = (ClientConnectionDelegate)res.getDelegate();

         if (cd != null)
         {
            // valid connection

            log.debug(this + " got local connection delegate " + cd);

            ConnectionState state = (ConnectionState)((DelegateSupport)cd).getState();
            FailoverCommandCenter fcc = state.getFailoverCommandCenter();

            // add a connection listener to detect failure; the consolidated remoting connection
            // listener must be already in place and configured
            state.getRemotingConnection().getConnectionListener().
               addDelegateListener(new ConnectionFailureListener(fcc, state.getRemotingConnection()));

            state.configureFailoverCommandCenter();

            log.debug(this + " installed failure listener on " + cd);

            // also cache the username and the password into state, useful in case
            // FailoverCommandCenter needs to create a new connection instead of a failed on
            state.setUsername(username);
            state.setPassword(password);

            // also add a reference to the clustered ConnectionFactory delegate, useful in case
            // FailoverCommandCenter needs to create a new connection instead of a failed on
            state.setClusteredConnectionFactoryDeleage(clusteredDelegate);

            return new CreateConnectionResult(cd);
         }
         else
         {
            // we did not get a valid connection to the node we've just tried

            int actualServerID = res.getActualFailoverNodeID();

            if (actualServerID == -1)
            {
               // No failover attempt was detected on the server side; this might happen if the
               // client side network fails temporarily so the client connection breaks but the
               // server cluster is still up and running - in this case we don't perform failover.

               // TODO Is this the right thing to do?

               log.warn("Client attempted failover, but no failover attempt " +
                        "has been detected on the server side.");

               return null;
            }

            // Server side failover has occurred / is occurring but trying to go to the 'default'
            // failover node did not succeed. Retry with the node suggested by the cluster.

            attemptCount++;

            delegate = getDelegateForNode(actualServerID);

            if (delegate == null)
            {
               // the delegate corresponding to the actualServerID not found among the cached
               // delegates. TODO Could this ever happen? Should we send back the cf, or update it
               // instead of just the id??
               throw new JMSException("Cannot find a cached connection factory delegate for " +
                  "node " + actualServerID);
            }
         }
      }

      throw new JMSException("Maximum number of failover attempts exceeded. " +
                             "Cannot find a server to failover onto.");
   }

   public String toString()
   {
      return "ClusteringAspect[" + clusteredDelegate + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------


   /**
    * TODO This is currently hardcoded as round robin. The policy should be pluggable.
    */
   private synchronized ClientConnectionFactoryDelegate getNextRoundRobinDelegate()
   {
      ClientConnectionFactoryDelegate[] delegates = clusteredDelegate.getDelegates();

      if (next >= delegates.length)
      {
         next = 0;
      }

      ClientConnectionFactoryDelegate delegate = delegates[next++];

         if (next >= delegates.length)
         {
            next = 0;
         }

      return delegate;
   }

   private synchronized ClientConnectionFactoryDelegate getFailoverDelegateForNode(Integer nodeID)
   {
      ClientConnectionFactoryDelegate[] delegates = clusteredDelegate.getDelegates();

      if (nodeID.intValue() < 0)
      {
         throw new IllegalArgumentException("nodeID must be 0 or positive");
      }

      Map failoverMap = clusteredDelegate.getFailoverMap();
      Integer failoverNodeID = (Integer)failoverMap.get(nodeID);

      for (int i = 0; i < delegates.length; i++)
      {
         if (delegates[i].getServerID() == failoverNodeID.intValue())
         {
            return delegates[i];
         }
      }

      return null;
   }

   private synchronized ClientConnectionFactoryDelegate getDelegateForNode(int nodeID)
   {
      ClientConnectionFactoryDelegate[] delegates = clusteredDelegate.getDelegates();

      for (int i = 0; i < delegates.length; i++)
      {
         if (delegates[i].getServerID() == nodeID)
         {
            return delegates[i];
         }
      }

      return null;
   }

   // Inner classes --------------------------------------------------------------------------------

}

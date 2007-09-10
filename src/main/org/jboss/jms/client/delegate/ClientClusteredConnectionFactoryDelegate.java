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
package org.jboss.jms.client.delegate;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import javax.jms.JMSException;

import org.jboss.jms.client.plugin.LoadBalancingPolicy;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.remoting.ConnectionFactoryCallbackHandler;
import org.jboss.jms.client.container.JMSClientVMIdentifier;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.CreateConnectionResult;
import org.jboss.jms.delegate.IDBlock;
import org.jboss.jms.delegate.TopologyResult;
import org.jboss.jms.exception.MessagingNetworkFailureException;
import org.jboss.jms.wireformat.ConnectionFactoryAddCallbackRequest;
import org.jboss.jms.wireformat.ConnectionFactoryGetTopologyRequest;
import org.jboss.jms.wireformat.ConnectionFactoryGetTopologyResponse;
import org.jboss.jms.wireformat.ConnectionFactoryRemoveCallbackRequest;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.Version;
import org.jboss.messaging.util.WeakHashSet;

/**
 * A ClientClusteredConnectionFactoryDelegate.
 *
 * It DOESN'T extend DelegateSupport, because none of DelegateSupport's attributes make sense here:
 * there is no corresponding enpoint on the server, there's no ID, etc. This is just a "shallow"
 * delegate, that in turn delegates to its sub-delegates (ClientConnectionFactoryDelegate instances)
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientClusteredConnectionFactoryDelegate extends DelegateSupport
   implements Serializable, ConnectionFactoryDelegate
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 8286850860206289277L;

   private static final Logger log =
      Logger.getLogger(ClientClusteredConnectionFactoryDelegate.class);
   private static boolean trace = log.isTraceEnabled();

   // Serialization and CallbackHandler code -------------------------------------------------------

   private transient JMSRemotingConnection remoting;
   private transient ClientConnectionFactoryDelegate currentDelegate;

   private void readObject(java.io.ObjectInputStream s)
        throws java.io.IOException, ClassNotFoundException
   {
      s.defaultReadObject();
      establishCallback();
   }

   public synchronized void establishCallback()
   {

      log.debug(" Establishing CFCallback\n");

      for (int server = delegates.length - 1; server >= 0; server--)
      {
         if (trace) log.trace("Closing current callback");
         closeCallback();

         if (trace) log.trace("Trying communication on server(" + server + ")=" + delegates[server].getServerLocatorURI());
         try
         {
            remoting = new JMSRemotingConnection(delegates[server].getServerLocatorURI(), true);
            remoting.start();
            currentDelegate = delegates[server];
            if (trace) log.trace("Adding callback");
            addCallback(delegates[server]);
            if (trace) log.trace("Getting topology");
            TopologyResult topology = getTopology();
            if (trace) log.trace("delegates.size = " + topology.getDelegates().length);
            addShutdownHook();

            break;
         }
         catch (Throwable e)
         {
            log.warn("Server communication to server[" + server + "] (" +
               delegates[server].getServerLocatorURI() + ") during establishCallback was broken, " +
               "trying the next one", e);
            if (remoting != null)
            {
               remoting.stop();
               remoting = null;
               currentDelegate = null;
            }
         }
      }
   }

   private void addCallback(ClientConnectionFactoryDelegate delegate) throws Throwable
   {
      remoting.getCallbackManager().setConnectionfactoryCallbackHandler(new ConnectionFactoryCallbackHandler(this, remoting));

      ConnectionFactoryAddCallbackRequest request =
         new ConnectionFactoryAddCallbackRequest (JMSClientVMIdentifier.instance,
               remoting.getRemotingClient().getSessionId(),
               delegate.getID(),
               Version.instance().getProviderIncrementingVersion());

      remoting.getRemotingClient().invoke(request, null);

   }

   private void addShutdownHook()
   {
      finalizerHook.addDelegate(this);
   }

   private void removeCallback() throws Throwable
   {
      ConnectionFactoryRemoveCallbackRequest request =
         new ConnectionFactoryRemoveCallbackRequest (JMSClientVMIdentifier.instance,
               remoting.getRemotingClient().getSessionId(),
               currentDelegate.getID(),
               Version.instance().getProviderIncrementingVersion());

      remoting.getRemotingClient().invoke(request, null);
   }

   protected void finalize() throws Throwable
   {
      super.finalize();
      closeCallback();

   }

   public void closeCallback()
   {
      if (remoting != null)
      {
         try
         {
            removeCallback();
         }
         catch (Throwable warn)
         {
            log.warn(warn, warn);
         }

         try
         {
            remoting.removeConnectionListener();
            remoting.stop();
            currentDelegate = null;
         }
         catch (Throwable ignored)
         {
         }

         remoting = null;
      }
   }
   // Serialization and CallbackHandler code -------------------------------------------------------

   
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private String uniqueName;

   private ClientConnectionFactoryDelegate[] delegates;

   // Map <Integer(nodeID)->Integer(failoverNodeID)>
   private Map failoverMap;

   private LoadBalancingPolicy loadBalancingPolicy;
   
   private boolean supportsFailover;
   
   private boolean supportsLoadBalancing;

   // Constructors ---------------------------------------------------------------------------------

   public ClientClusteredConnectionFactoryDelegate(String uniqueName,
                                                   ClientConnectionFactoryDelegate[] delegates,
                                                   Map failoverMap,
                                                   LoadBalancingPolicy loadBalancingPolicy,
                                                   boolean supportsFailover)
   {
      this.uniqueName = uniqueName;
      this.delegates = delegates;
      this.failoverMap = failoverMap;
      this.loadBalancingPolicy = loadBalancingPolicy;
      this.supportsFailover = supportsFailover;
   }

   // ConnectionFactoryDelegate implementation -----------------------------------------------------

   public synchronized byte[] getClientAOPStack() throws JMSException
   {
      // Use one of the non-clustered ConnectionFactory delegates to retrieve the client AOP stack
      // from one of the nodes.
      
      // It doesn't really matter which one
      
      log.trace("Getting AOP stack, there are " + delegates.length + " delegates to choose from");
            
      for (int server = 0; server < delegates.length; server++)
      {
         try
         {
            ConnectionFactoryDelegate aopStackProvider = delegates[server];

            log.trace("getting AOP stack from " + aopStackProvider);

            return aopStackProvider.getClientAOPStack();
         }
         catch (MessagingNetworkFailureException e)
         {
            log.warn("Server" + server + " was broken, loading AOP from next delegate", e);
         }
      }

      throw new MessagingNetworkFailureException("Failed to download and/or install client side AOP stack");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public CreateConnectionResult createConnectionDelegate(String username, String password,
                                                          int failedNodeID) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public IDBlock getIdBlock(int size) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }

   // Public ---------------------------------------------------------------------------------------
   
   public ClientConnectionFactoryDelegate[] getDelegates()
   {
      return delegates;
   }

   public void setDelegates(ClientConnectionFactoryDelegate[] dels)
   {
      this.delegates = dels;
      loadBalancingPolicy.updateView(dels);
   }

   public Map getFailoverMap()
   {
      return failoverMap;
   }

   public void setFailoverMap(Map failoverMap)
   {
      this.failoverMap = failoverMap;
   }

   public LoadBalancingPolicy getLoadBalancingPolicy()
   {
      return loadBalancingPolicy;
   }
   
   public boolean isSupportsFailover()
   {
   	return supportsFailover;
   }

   public String getUniqueName()
   {
      return uniqueName;
   }


   public TopologyResult getTopology() throws JMSException
   {

      try
      {
         ConnectionFactoryGetTopologyRequest request =
            new ConnectionFactoryGetTopologyRequest(currentDelegate.getID());

         ConnectionFactoryGetTopologyResponse response = (ConnectionFactoryGetTopologyResponse)remoting.getRemotingClient().invoke(request, null);


         TopologyResult topology = (TopologyResult)response.getResponse();

         updateFailoverInfo(topology.getDelegates(), topology.getFailoverMap());

         return topology;
      }
      catch (Throwable e)
      {
         throw handleThrowable(e);
      }
   }

   //Only used in testing
   public void setSupportsFailover(boolean failover)
   {
   	this.supportsFailover = failover;
   }
   
   /** Method used to update the delegate and failoverMap during viewChange */
   public synchronized void updateFailoverInfo(ClientConnectionFactoryDelegate[] delegates,
                                               Map newFailoverMap)
   {	
   	log.trace("Updating failover info " + delegates.length + " map " + newFailoverMap);
   	
      this.delegates = delegates;
      
      //Note! We do not overwrite the failoverMap, we *add* to it, see http://jira.jboss.com/jira/browse/JBMESSAGING-1041
      
      failoverMap.putAll(newFailoverMap);

      loadBalancingPolicy.updateView(delegates);
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("ClusteredConnectionFactoryDelegate[");
      if (delegates == null)
      {
         sb.append("-]");
      }
      else
      {
         sb.append("SIDs={");
         for(int i = 0; i < delegates.length; i++)
         {
            sb.append(delegates[i].getServerID());
            if (i < delegates.length - 1)
            {
               sb.append(',');
            }
         }
         sb.append("}]");
      }
      return sb.toString();
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------

   static FinalizerShutdownHook finalizerHook;

   static
   {
      finalizerHook = new FinalizerShutdownHook();
      Runtime.getRuntime().addShutdownHook(finalizerHook);

   }


   // A Single ShutdownHook for the entire class
   static class FinalizerShutdownHook extends Thread
   {

      Set<ClientClusteredConnectionFactoryDelegate> delegates;

      public FinalizerShutdownHook()
      {
         delegates = Collections.synchronizedSet(new WeakHashSet());
      }

      public void addDelegate(ClientClusteredConnectionFactoryDelegate delegate)
      {
         delegates.add(delegate);
      }

      public void run()
      {
         for (ClientClusteredConnectionFactoryDelegate delegate: delegates)
         {
            try
            {
               delegate.finalize();
            }
            catch (Throwable ignored)
            {
            }
         }
      }
   }

}

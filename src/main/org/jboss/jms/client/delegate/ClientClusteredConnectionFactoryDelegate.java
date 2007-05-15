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

import javax.jms.JMSException;

import org.jboss.jms.client.plugin.LoadBalancingPolicy;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.CreateConnectionResult;
import org.jboss.jms.delegate.IDBlock;
import org.jboss.jms.exception.MessagingNetworkFailureException;
import org.jboss.logging.Logger;

/**
 * A ClientClusteredConnectionFactoryDelegate.
 *
 * It DOESN'T extend DelegateSupport, because none of DelegateSupport's attributes make sense here:
 * there is no corresponding enpoint on the server, there's no ID, etc. This is just a "shallow"
 * delegate, that in turn delegates to its sub-delegates (ClientConnectionFactoryDelegate instances)
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientClusteredConnectionFactoryDelegate
   implements Serializable, ConnectionFactoryDelegate
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 8286850860206289277L;

   private static final Logger log =
      Logger.getLogger(ClientClusteredConnectionFactoryDelegate.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private ClientConnectionFactoryDelegate[] delegates;

   // Map <Integer(nodeID)->Integer(failoverNodeID)>
   private Map failoverMap;

   private LoadBalancingPolicy loadBalancingPolicy;

   // Constructors ---------------------------------------------------------------------------------

   public ClientClusteredConnectionFactoryDelegate(ClientConnectionFactoryDelegate[] delegates,
                                                   Map failoverMap,
                                                   LoadBalancingPolicy loadBalancingPolicy)
   {
      this.delegates = delegates;
      this.failoverMap = failoverMap;
      this.loadBalancingPolicy = loadBalancingPolicy;
   }

   // ConnectionFactoryDelegate implementation -----------------------------------------------------

   public synchronized byte[] getClientAOPStack() throws JMSException
   {
      // Use one of the non-clustered ConnectionFactory delegates to retrieve the client AOP stack
      // from one of the nodes.
      
      // It doesn't really matter which one
      
      log.debug("Getting AOP stack, there are " + delegates.length + " delegates to choose from");
            
      for (int server = 0; server < delegates.length; server++)
      {
         try
         {
            ConnectionFactoryDelegate aopStackProvider = delegates[server];

            log.debug("getting AOP stack from " + aopStackProvider);

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

   /** Method used to update the delegate and failoverMap during viewChange */
   public synchronized void updateFailoverInfo(ClientConnectionFactoryDelegate[] delegates,
                                               Map failoverMap)
   {
      this.delegates = delegates;
      this.failoverMap = failoverMap;

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

}

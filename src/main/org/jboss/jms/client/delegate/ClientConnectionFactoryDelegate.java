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

import javax.jms.JMSException;

import org.jboss.aop.Dispatcher;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;

/**
 * The client-side ConnectionFactory delegate class.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientConnectionFactoryDelegate
   extends DelegateSupport implements ConnectionFactoryDelegate
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 2512460695662741413L;
   
   private static final Logger log = Logger.getLogger(ClientConnectionFactoryDelegate.class);

   
   // Attributes ----------------------------------------------------
    
   protected String serverLocatorURI;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   public ClientConnectionFactoryDelegate(int objectID, String serverLocatorURI)
   {
      super(objectID);
      
      this.serverLocatorURI = serverLocatorURI;
   }
   
   // ConnectionFactoryDelegateImplementation -----------------------
   
   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public ConnectionDelegate createConnectionDelegate(String username, String password)
      throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");
   }
   
   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public byte[] getClientAOPConfig()
   {      
      throw new IllegalStateException("This invocation should not be handled here!");
   }
   
   // Public --------------------------------------------------------
    
   public Object invoke(Invocation invocation) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("invoking " + ((MethodInvocation)invocation).getMethod().getName() + " on server"); }

      invocation.getMetaData().addMetaData(Dispatcher.DISPATCHER,
                                           Dispatcher.OID,
                                           new Integer(id), PayloadKey.AS_IS);
      
      String methodName = ((MethodInvocation)invocation).getMethod().getName();
      
      Object ret = null;
      
      if ("getClientAOPConfig".equals(methodName))
      {
         //This is invoked on it's own connection
         InvokerLocator locator = new InvokerLocator(serverLocatorURI);
         
         Client client = new Client(locator);
         
         try
         {            
            ret = client.invoke(invocation, null);
            
            //invocation.setResponseContextInfo(response.getContextInfo());
         }
         finally
         {
            client.disconnect();
         }
      }
      else if ("createConnectionDelegate".equals(methodName))
      {
         //This must be invoked on the same connection subsequently used by the created JMS connection
         JMSRemotingConnection connection = new JMSRemotingConnection(serverLocatorURI);
         
         MethodInvocation mi = (MethodInvocation)invocation;
         
         Client client = connection.getInvokingClient();
         
         //I will need this on the server-side to create the ConsumerDelegate instance
         mi.getMetaData().addMetaData(MetaDataConstants.JMS,
                                      MetaDataConstants.REMOTING_SESSION_ID,
                                      client.getSessionId(), PayloadKey.AS_IS);
             
         try
         {            
            ret = client.invoke(invocation, null);
             
            ClientConnectionDelegate delegate = (ClientConnectionDelegate)ret;
            
            delegate.setConnnectionState(connection);
         }
         catch (Throwable t)
         {
            connection.close();
            
            throw t;
         } 
      }
      else
      {
         throw new IllegalStateException("Unknown invocation");
      }

      if (log.isTraceEnabled()) { log.trace("got server response for " + ((MethodInvocation)invocation).getMethod().getName()); }

      return ret;
   }
   
   
   public String toString()
   {
      return "ConnectionFactoryDelegate[" + id + "]";
   }
   
   // Protected -----------------------------------------------------
   
   protected Client getClient()
   {
      return null;
   }
   
   // Package Private -----------------------------------------------
      
   // Private -------------------------------------------------------
      
   // Inner Classes -------------------------------------------------
      
}

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
import org.jboss.jms.client.container.JMSClientVMIdentifier;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.server.Version;
import org.jboss.jms.server.remoting.MessagingMarshallable;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.IdBlock;
import org.jboss.remoting.Client;

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

   private JMSRemotingConnection nextConnection;

   protected String serverLocatorURI;
   protected Version serverVersion;
   protected String serverID;
   protected transient Client client;
   protected boolean clientPing;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   public ClientConnectionFactoryDelegate(int objectID, String serverLocatorURI,
                                          Version serverVersion, String serverID,
                                          boolean clientPing)
   {
      super(objectID);
      
      this.serverLocatorURI = serverLocatorURI;
      this.serverVersion = serverVersion;
      this.serverID = serverID;
      this.clientPing = clientPing;
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
   
   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public IdBlock getIdBlock(int size)
   {      
      throw new IllegalStateException("This invocation should not be handled here!");
   }
   
   // Public --------------------------------------------------------
    
   public synchronized Object invoke(Invocation invocation) throws Throwable
   {
      String methodName = ((MethodInvocation)invocation).getMethod().getName();
      if (log.isTraceEnabled()) { log.trace("invoking " + methodName + " on server"); }
      
      invocation.getMetaData().addMetaData(Dispatcher.DISPATCHER,
                                           Dispatcher.OID,
                                           new Integer(id),
                                           PayloadKey.AS_IS);

      Object ret = null;

      if ("createConnectionDelegate".equals(methodName))
      {         
         // this must be invoked on the same connection subsequently used by the created JMS
         // connection
         JMSRemotingConnection remotingConnection = getRemotingConnection();
         Client client = remotingConnection.getInvokingClient();
         nextConnection = null;

         MethodInvocation mi = (MethodInvocation)invocation;

         mi.getMetaData().addMetaData(MetaDataConstants.JMS,
                                      MetaDataConstants.REMOTING_SESSION_ID,
                                      client.getSessionId(),
                                      PayloadKey.AS_IS);
         
         mi.getMetaData().addMetaData(MetaDataConstants.JMS,
                                      MetaDataConstants.JMS_CLIENT_VM_ID,
                                      JMSClientVMIdentifier.instance,
                                      PayloadKey.AS_IS);
         try
         {
            MessagingMarshallable request =
               new MessagingMarshallable(getVersionToUse().getProviderIncrementingVersion(), mi);
            MessagingMarshallable response = (MessagingMarshallable)client.invoke(request, null);

            ret = response.getLoad();
            ClientConnectionDelegate delegate = (ClientConnectionDelegate)ret;
            delegate.setRemotingConnection(remotingConnection);
         }
         catch (Throwable t)
         {
            remotingConnection.close();
            throw t;
         } 
      }
      else
      {         
         byte v = getVersionToUse().getProviderIncrementingVersion();
            
         Client cl = getRemotingConnection().getInvokingClient();

         MessagingMarshallable request = new MessagingMarshallable(v, invocation);
         MessagingMarshallable response = (MessagingMarshallable)cl.invoke(request, null);
         ret = response.getLoad();
      }

      if (log.isTraceEnabled()) { log.trace("got server response for " + ((MethodInvocation)invocation).getMethod().getName()); }

      return ret;
   }
   
   
   public String toString()
   {
      return "ConnectionFactoryDelegate[" + id + "]";
   }
   
   public Version getServerVersion()
   {
      return serverVersion;
   }
   
   public String getServerID()
   {
      return serverID;
   }
   
   public Version getVersionToUse()
   {
      Version clientVersion = Version.instance();
      
      Version versionToUse;
      
      if (serverVersion.getProviderIncrementingVersion() <= clientVersion.getProviderIncrementingVersion())
      {
         versionToUse = serverVersion;
      }
      else
      {
         versionToUse = clientVersion;
      }
      
      return versionToUse;
   }
   
   public String getServerLocatorURI()
   {
      return serverLocatorURI;
   }
   
   // Protected -----------------------------------------------------
   
   protected JMSRemotingConnection getRemotingConnection() throws Throwable
   {
      if (nextConnection == null)
      {         
         nextConnection = new JMSRemotingConnection(serverLocatorURI, clientPing);
      }
      return nextConnection;
   }
   
   protected Client getClient()
   {
      return null;
   }
   
   // Package Private -----------------------------------------------
      
   // Private -------------------------------------------------------
      
   // Inner Classes -------------------------------------------------
      
}

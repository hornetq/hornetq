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
import org.jboss.aop.metadata.SimpleMetaData;
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

   protected String serverLocatorURI;
   protected Version serverVersion;
   protected String serverID;
   protected transient Client client;
   protected boolean clientPing;

   private JMSRemotingConnection remotingConnection;
   private boolean trace;

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
      this.remotingConnection = null;

      trace = log.isTraceEnabled();
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
      MethodInvocation mi = (MethodInvocation)invocation;
      String methodName = mi.getMethod().getName();

      if (trace) { log.trace("invoking " + methodName + " on server"); }

      SimpleMetaData md = mi.getMetaData();

      md.addMetaData(Dispatcher.DISPATCHER,
                     Dispatcher.OID,
                     new Integer(id),
                     PayloadKey.AS_IS);

      if (remotingConnection == null)
      {
         remotingConnection = new JMSRemotingConnection(serverLocatorURI, clientPing);
      }

      if (log.isTraceEnabled()) { log.trace(this + " using " + remotingConnection); }

      Client client = remotingConnection.getInvokingClient();

      if ("createConnectionDelegate".equals(methodName))
      {
         md.addMetaData(MetaDataConstants.JMS,
                        MetaDataConstants.REMOTING_SESSION_ID,
                        client.getSessionId(),
                        PayloadKey.AS_IS);

         md.addMetaData(MetaDataConstants.JMS,
                        MetaDataConstants.JMS_CLIENT_VM_ID,
                        JMSClientVMIdentifier.instance,
                        PayloadKey.AS_IS);
      }

      byte v = getVersionToUse().getProviderIncrementingVersion();
      MessagingMarshallable request = new MessagingMarshallable(v, mi);
      MessagingMarshallable response = null;

      try
      {
         response = (MessagingMarshallable)client.invoke(request, null);
         if (trace) { log.trace("got server response for " + methodName); }

      }
      catch (Throwable t)
      {
         remotingConnection.close();
         remotingConnection = null;
         throw t;
      }

      Object ret = response.getLoad();
      if (ret instanceof ClientConnectionDelegate)
      {
         ClientConnectionDelegate connectionDelegate = (ClientConnectionDelegate)ret;
         connectionDelegate.setRemotingConnection(remotingConnection);
         remotingConnection = null;
      }

      return ret;
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

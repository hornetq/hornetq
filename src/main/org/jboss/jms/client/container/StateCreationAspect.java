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
package org.jboss.jms.client.container;

import javax.jms.Destination;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.client.delegate.ClientProducerDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.remoting.JMSRemotingConnection;
import org.jboss.jms.client.state.BrowserState;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.ConsumerState;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.client.state.ProducerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConnectionFactoryDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.MessageIdGenerator;
import org.jboss.jms.message.MessageIdGeneratorFactory;
import org.jboss.jms.server.Version;
import org.jboss.jms.server.endpoint.CreateConnectionResult;
import org.jboss.logging.Logger;

/**
 * Maintains the hierarchy of parent and child state objects. For each delegate, this interceptor
 * maintains a state object and it's children/parent. The state object is then made accessible to
 * any of the aspects/interceptors in the chain. This enables the aspects/interceptors to access
 * and make use of the state without having to resort to multiple messy get/set methods on the
 * delegate API.
 * 
 * This interceptor is PER_VM.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * $Id$
 */
public class StateCreationAspect
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(StateCreationAspect.class);

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public Object handleCreateConnectionDelegate(Invocation inv) throws Throwable
   {
      ConnectionFactoryDelegate cfd = (ConnectionFactoryDelegate)inv.getTargetObject();

      CreateConnectionResult res = (CreateConnectionResult)inv.invokeNext();

      if(trace) { log.trace("got " + res + " on return"); }

      ClientConnectionDelegate connectionDelegate = (ClientConnectionDelegate)res.getDelegate();

      if (connectionDelegate != null)
      {
         connectionDelegate.init();

         int serverID = connectionDelegate.getServerID();
         Version versionToUse = connectionDelegate.getVersionToUse();
         JMSRemotingConnection remotingConn = connectionDelegate.getRemotingConnection();

         if (versionToUse == null)
         {
            throw new IllegalStateException("Connection version is null");
         }

         // We have one message id generator per unique server
         MessageIdGenerator g = MessageIdGeneratorFactory.instance.checkOutGenerator(serverID, cfd);

         ConnectionState connectionState =
            new ConnectionState(serverID, connectionDelegate, remotingConn, versionToUse, g);

         connectionDelegate.setState(connectionState);
      }

      return res;
   }

   public Object handleCreateSessionDelegate(Invocation invocation) throws Throwable
   {
      SessionDelegate sessionDelegate = (SessionDelegate)invocation.invokeNext();
      DelegateSupport delegate = (DelegateSupport)sessionDelegate;

      delegate.init();

      ConnectionState connectionState = (ConnectionState)getState(invocation);

      MethodInvocation mi = (MethodInvocation)invocation;
      boolean transacted = ((Boolean)mi.getArguments()[0]).booleanValue();
      int ackMode = ((Integer)mi.getArguments()[1]).intValue();
      boolean xa = ((Boolean)mi.getArguments()[2]).booleanValue();

      SessionState sessionState =
         new SessionState(connectionState, sessionDelegate, transacted, ackMode, xa);

      delegate.setState(sessionState);
      return delegate;
   }

   public Object handleCreateConsumerDelegate(Invocation invocation) throws Throwable
   {
      ClientConsumerDelegate consumerDelegate = (ClientConsumerDelegate)invocation.invokeNext();
      DelegateSupport delegate = (DelegateSupport)consumerDelegate;

      delegate.init();

      SessionState sessionState = (SessionState)getState(invocation);

      MethodInvocation mi = (MethodInvocation)invocation;
      Destination dest = (Destination)mi.getArguments()[0];
      String selector = (String)mi.getArguments()[1];
      boolean noLocal = ((Boolean)mi.getArguments()[2]).booleanValue();
      String subscriptionName = (String)mi.getArguments()[3];
      boolean connectionConsumer = ((Boolean)mi.getArguments()[4]).booleanValue();

      int consumerID = consumerDelegate.getID();

      int prefetchSize = consumerDelegate.getPrefetchSize();

      int maxDeliveries = consumerDelegate.getMaxDeliveries();

      long channelId = consumerDelegate.getChannelId();

      ConsumerState consumerState =
         new ConsumerState(sessionState, consumerDelegate, dest, selector, noLocal,
                           subscriptionName, consumerID, connectionConsumer, prefetchSize,
                           maxDeliveries, channelId);

      delegate.setState(consumerState);
      return consumerDelegate;
   }

   public Object handleCreateProducerDelegate(Invocation invocation) throws Throwable
   {
      // ProducerDelegates are not created on the server

      ProducerDelegate producerDelegate = new ClientProducerDelegate();
      DelegateSupport delegate = (DelegateSupport)producerDelegate;

      SessionState sessionState = (SessionState)getState(invocation);

      MethodInvocation mi = (MethodInvocation)invocation;
      Destination dest = ((Destination)mi.getArguments()[0]);


      ProducerState producerState = new ProducerState(sessionState, producerDelegate, dest);

      delegate.setState(producerState);

      // send an arbitrary invocation into the producer delegate, this will trigger AOP stack
      // initialization and AOP aspect class loading, using the "good" class loader, which is set
      // now. This will save us from having to switch the thread context class loader on every send.
      producerDelegate.getDeliveryMode();

      return producerDelegate;
   }

   public Object handleCreateBrowserDelegate(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;

      BrowserDelegate browserDelegate = (BrowserDelegate)invocation.invokeNext();
      DelegateSupport delegate = (DelegateSupport)browserDelegate;

      delegate.init();

      SessionState sessionState = (SessionState)getState(invocation);

      JBossDestination destination = (JBossDestination)mi.getArguments()[0];
      String selector = (String)mi.getArguments()[1];

      BrowserState state = new BrowserState(sessionState, browserDelegate, destination, selector);

      delegate.setState(state);
      return browserDelegate;
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   private HierarchicalState getState(Invocation inv)
   {
      return ((DelegateSupport)inv.getTargetObject()).getState();
   }

   // Inner Classes --------------------------------------------------
}


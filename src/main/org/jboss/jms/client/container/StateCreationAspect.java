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

import org.jboss.aop.Advised;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.client.state.BrowserState;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.ConsumerState;
import org.jboss.jms.client.state.HierarchicalState;
import org.jboss.jms.client.state.ProducerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.remoting.MetaDataConstants;

/**
 * Maintains the hierarchy of parent and child state objects. For each delegate, this interceptor
 * maintains a state object and it's children/parent. The state object is then made accessible to
 * any of the aspects/interceptors in the chain. This enables the aspects/interceptors to access
 * and make use of the state without having to resort to multiple messy get/set methods on the
 * delegate API.
 * 
 * This interceptor is PER_VM.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
 * $Id$
 */
public class StateCreationAspect
{
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public Object handleCreateConnectionDelegate(Invocation invocation) throws Throwable
   {
      ClientConnectionDelegate delegate = (ClientConnectionDelegate)invocation.invokeNext();
      
      delegate.init();
      
      delegate.setState(new ConnectionState(delegate,
                                            delegate.getServerID(),
                                            delegate.getServerLocatorURI()));
                  
      return delegate;
   }
   
   public Object handleCreateSessionDelegate(Invocation invocation) throws Throwable
   {
      SessionDelegate delegate = (SessionDelegate)invocation.invokeNext();

      DelegateSupport delegateSupport = (DelegateSupport)delegate;
      
      delegateSupport.init();
      
      ConnectionState connState = (ConnectionState)getState(invocation);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      boolean transacted = ((Boolean)mi.getArguments()[0]).booleanValue();
      int ackMode = ((Integer)mi.getArguments()[1]).intValue();
      boolean xa = ((Boolean)mi.getArguments()[2]).booleanValue();
      
      SessionState state = new SessionState(connState, delegate, transacted, ackMode, xa);
      
      delegateSupport.setState(state);
      
      return delegate;
   }
   
   public Object handleCreateConsumerDelegate(Invocation invocation) throws Throwable
   {
      ConsumerDelegate cons = (ConsumerDelegate)invocation.invokeNext();
      
      DelegateSupport delegate = (DelegateSupport)cons;
      
      delegate.init();
      
      SessionState sessState = (SessionState)getState(invocation);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      Destination dest = (Destination)mi.getArguments()[0];
      String selector = (String)mi.getArguments()[1];
      boolean noLocal = ((Boolean)mi.getArguments()[2]).booleanValue();    
      boolean connectionConsumer = ((Boolean)mi.getArguments()[4]).booleanValue();

      String consumerID =
         (String)((Advised)cons)._getInstanceAdvisor().getMetaData().
         getMetaData(MetaDataConstants.JMS, MetaDataConstants.CONSUMER_ID);
      
      ConsumerState state = new ConsumerState(sessState, cons, dest, selector, noLocal,
                                              consumerID, connectionConsumer);
      
      delegate.setState(state);
      
      return cons;
   }
   
   public Object handleCreateProducerDelegate(Invocation invocation) throws Throwable
   {
      ProducerDelegate prod = (ProducerDelegate)invocation.invokeNext();
      
      DelegateSupport delegate = (DelegateSupport)prod;
      
      delegate.init();
      
      SessionState sessState = (SessionState)getState(invocation);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Destination theDest = ((Destination)mi.getArguments()[0]);
      
      ProducerState state = new ProducerState(sessState, prod, theDest);
      
      delegate.setState(state);
      
      return prod;
   }
   
   public Object handleCreateBrowserDelegate(Invocation invocation) throws Throwable
   {
      BrowserDelegate browser = (BrowserDelegate)invocation.invokeNext();
      
      DelegateSupport delegate = (DelegateSupport)browser;
      
      delegate.init();
      
      SessionState sessState = (SessionState)getState(invocation);
      
      BrowserState state = new BrowserState(sessState, browser);
      
      delegate.setState(state);
      
      return browser;
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


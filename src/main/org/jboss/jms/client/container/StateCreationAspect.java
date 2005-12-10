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
import org.jboss.jms.client.stubs.ClientStubBase;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.remoting.MetaDataConstants;

/**
 * Maintains the hierarchy of parent and child state objects
 * For each delegate, this interceptor maintains a state object
 * and it's children/parent.
 * The state object is then made accessible to any of the aspects/interceptors
 * in the chain.
 * This enables the aspects/interceptors to access and make use of the state
 * without having to resort to multiple messy get/set methods on the delegate API
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
      ConnectionDelegate conn = (ConnectionDelegate)invocation.invokeNext();
      
      ClientStubBase csb = (ClientStubBase)conn;
      
      csb.init();
      
      csb.setState(new ConnectionState(conn));           
                  
      return conn;
   }
   
   public Object handleCreateSessionDelegate(Invocation invocation) throws Throwable
   {
      SessionDelegate sess = (SessionDelegate)invocation.invokeNext();
      
      ClientStubBase csb = (ClientStubBase)sess;
      
      csb.init();
      
      ConnectionState connState = (ConnectionState)getState(invocation);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      boolean transacted = ((Boolean)mi.getArguments()[0]).booleanValue();
      int ackMode = ((Integer)mi.getArguments()[1]).intValue();
      boolean xa = ((Boolean)mi.getArguments()[2]).booleanValue();
      
      SessionState state = new SessionState(connState, sess, transacted, ackMode, xa);
      
      csb.setState(state);
      
      return sess;
   }
   
   public Object handleCreateConsumerDelegate(Invocation invocation) throws Throwable
   {
      ConsumerDelegate cons = (ConsumerDelegate)invocation.invokeNext();
      
      ClientStubBase csb = (ClientStubBase)cons;
      
      csb.init();
      
      SessionState sessState = (SessionState)getState(invocation);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      Destination dest = (Destination)mi.getArguments()[0];
      String selector = (String)mi.getArguments()[1];
      boolean noLocal = ((Boolean)mi.getArguments()[2]).booleanValue();    
      boolean connectionConsumer = ((Boolean)mi.getArguments()[4]).booleanValue();      
      String consumerID = 
         (String)((Advised)cons)._getInstanceAdvisor().getMetaData().getMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.CONSUMER_ID);
      
      ConsumerState state = new ConsumerState(sessState, cons, dest, selector, noLocal, consumerID, connectionConsumer);
      
      csb.setState(state);
      
      return cons;
   }
   
   public Object handleCreateProducerDelegate(Invocation invocation) throws Throwable
   {
      ProducerDelegate prod = (ProducerDelegate)invocation.invokeNext();
      
      ClientStubBase csb = (ClientStubBase)prod;
      
      csb.init();
      
      SessionState sessState = (SessionState)getState(invocation);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Destination theDest = ((Destination)mi.getArguments()[0]);
      
      ProducerState state = new ProducerState(sessState, prod, theDest);
      
      csb.setState(state);
      
      return prod;
   }
   
   public Object handleCreateBrowserDelegate(Invocation invocation) throws Throwable
   {
      BrowserDelegate browser = (BrowserDelegate)invocation.invokeNext();
      
      ClientStubBase csb = (ClientStubBase)browser;      
      
      csb.init();
      
      SessionState sessState = (SessionState)getState(invocation);
      
      BrowserState state = new BrowserState(sessState, browser);
      
      csb.setState(state);
      
      return browser;
   }
      
   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------
   
   private HierarchicalState getState(Invocation inv)
   {
      return ((ClientStubBase)inv.getTargetObject()).getState();
   }
   
   // Inner Classes --------------------------------------------------
}


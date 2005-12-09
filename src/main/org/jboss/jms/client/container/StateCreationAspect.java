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
import org.jboss.aop.util.PayloadKey;
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
import org.jboss.logging.Logger;

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
   
   private static final Logger log = Logger.getLogger(StateCreationAspect.class);
   

   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public Object handleCreateConnectionDelegate(Invocation invocation) throws Throwable
   {
      ConnectionDelegate conn = (ConnectionDelegate)invocation.invokeNext();
      
      ((ClientStubBase)conn).init();
      
      ConnectionState state = new ConnectionState(conn);      

      setState(conn, state);
      
      return conn;
   }
   
   public Object handleCreateSessionDelegate(Invocation invocation) throws Throwable
   {
      SessionDelegate sess = (SessionDelegate)invocation.invokeNext();
      
      ((ClientStubBase)sess).init();
      
      ConnectionState connState = (ConnectionState)getState(invocation);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      boolean transacted = ((Boolean)mi.getArguments()[0]).booleanValue();
      int ackMode = ((Integer)mi.getArguments()[1]).intValue();
      boolean xa = ((Boolean)mi.getArguments()[2]).booleanValue();
      
      SessionState state = new SessionState(connState, sess, transacted, ackMode, xa);
      
      setState(sess, state);
      
      return sess;
   }
   
   public Object handleCreateConsumerDelegate(Invocation invocation) throws Throwable
   {
      ConsumerDelegate cons = (ConsumerDelegate)invocation.invokeNext();
      
      ((ClientStubBase)cons).init();
      
      SessionState sessState = (SessionState)getState(invocation);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      Destination dest = (Destination)mi.getArguments()[0];
      String selector = (String)mi.getArguments()[1];
      boolean noLocal = ((Boolean)mi.getArguments()[2]).booleanValue();    
      boolean connectionConsumer = ((Boolean)mi.getArguments()[4]).booleanValue();      
      String consumerID = 
         (String)((Advised)cons)._getInstanceAdvisor().getMetaData().getMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.CONSUMER_ID);
      
      ConsumerState state = new ConsumerState(sessState, cons, dest, selector, noLocal, consumerID, connectionConsumer);
      
      setState(cons, state);
      
      return cons;
   }
   
   public Object handleCreateProducerDelegate(Invocation invocation) throws Throwable
   {
      ProducerDelegate prod = (ProducerDelegate)invocation.invokeNext();
      
      ((ClientStubBase)prod).init();
      
      SessionState sessState = (SessionState)getState(invocation);
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Destination theDest = ((Destination)mi.getArguments()[0]);
      
      ProducerState state = new ProducerState(sessState, prod, theDest);
      
      setState(prod, state);
      
      return prod;
   }
   
   public Object handleCreateBrowserDelegate(Invocation invocation) throws Throwable
   {
      BrowserDelegate browser = (BrowserDelegate)invocation.invokeNext();
      
      ((ClientStubBase)browser).init();
      
      SessionState sessState = (SessionState)getState(invocation);
      
      BrowserState state = new BrowserState(sessState, browser);
      
      setState(browser, state);
      
      return browser;
   }
      
   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------
   
   private void setState(Object delegate, HierarchicalState state)
   {
      ((Advised)delegate)._getInstanceAdvisor().getMetaData()
         .addMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.LOCAL_STATE, state, PayloadKey.TRANSIENT);     
   }
   
   private HierarchicalState getState(Invocation inv)
   {
      return (HierarchicalState)inv.getMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.LOCAL_STATE);
   }
   
   // Inner Classes --------------------------------------------------
}


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

import org.jboss.aop.Advised;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.remoting.Remoting;
import org.jboss.jms.client.state.ConsumerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.client.stubs.ClientStubBase;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.remoting.MetaDataConstants;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.transport.Connector;

/**
 * 
 * Handles operations related to the consumer.
 * 
 * This aspect is PER_VM.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConsumerAspect
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ConsumerAspect.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Object handleCreateConsumerDelegate(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;
      
      //register/unregister a callback handler that deal with callbacks sent by the server

      InvokerLocator serverLocator = (InvokerLocator)invocation.
            getMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.INVOKER_LOCATOR);

      if (serverLocator == null)
      {
         throw new RuntimeException("No InvokerLocator supplied. Can't invoke remotely!");
      }
      
      // TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)
      Connector callbackServer = Remoting.getCallbackServer();
      
      InvokerLocator callbackServerLocator = callbackServer.getLocator();
      
      if (log.isTraceEnabled())
      {
         log.trace("Callback server listening on " + callbackServerLocator);
      }

      boolean isCC = ((Boolean)mi.getArguments()[4]).booleanValue();

      //Create the message handler
      SessionState sessState = (SessionState)((ClientStubBase)invocation.getTargetObject()).getState();
      
      MessageCallbackHandler messageHandler = new MessageCallbackHandler(isCC, sessState.getAcknowledgeMode());

      Client client = new Client(serverLocator, "JMS");
      
      client.addListener(messageHandler, callbackServerLocator);

      // I will need this on the server-side to create the ConsumerDelegate instance
      invocation.getMetaData().addMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.REMOTING_SESSION_ID,
                                     client.getSessionId(), PayloadKey.AS_IS);

      ConsumerDelegate consumerDelegate = (ConsumerDelegate)invocation.invokeNext();
      
      SessionDelegate del = (SessionDelegate)invocation.getTargetObject();

      ConsumerState theState = (ConsumerState)((ClientStubBase)consumerDelegate).getState();
         
      messageHandler.setSessionDelegate(del);
      messageHandler.setConsumerDelegate(consumerDelegate);
      messageHandler.setConsumerID(theState.getConsumerID());
      messageHandler.setCallbackServer(callbackServer, client); // TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)

      //Add to the instance advisor's metadata
      SimpleMetaData instanceMetaData = ((Advised)consumerDelegate)._getInstanceAdvisor().getMetaData();
      
      instanceMetaData.addMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.MESSAGE_HANDLER, messageHandler, PayloadKey.TRANSIENT);
      
      return consumerDelegate;
   }
   
   public Object handleClosing(Invocation invocation) throws Throwable
   {
      MessageCallbackHandler handler =
         (MessageCallbackHandler)invocation.getMetaData(MetaDataConstants.TAG_NAME, MetaDataConstants.MESSAGE_HANDLER);
      
      if (handler == null)
      {
         throw new IllegalStateException("Cannot find message handler");
      }
      
      handler.close();  
      
      return invocation.invokeNext();
   }
   
   public Object handleGetDestination(Invocation invocation) throws Throwable
   {
      return getState(invocation).getDestination();
   }
   
   public Object handleGetNoLocal(Invocation invocation) throws Throwable
   {
      return getState(invocation).isNoLocal() ? Boolean.TRUE : Boolean.FALSE;
   }
   
   public Object handleGetMessageSelector(Invocation invocation) throws Throwable
   {
      return getState(invocation).getSelector();
   }
   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   private ConsumerState getState(Invocation inv)
   {
      return (ConsumerState)((ClientStubBase)inv.getTargetObject()).getState();
   }
   
   // Inner classes -------------------------------------------------
}

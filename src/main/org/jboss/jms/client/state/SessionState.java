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
package org.jboss.jms.client.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.Collections;

import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.client.delegate.ClientProducerDelegate;
import org.jboss.jms.client.delegate.ClientBrowserDelegate;
import org.jboss.jms.client.remoting.MessageCallbackHandler;
import org.jboss.jms.client.remoting.CallbackManager;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.Version;
import org.jboss.jms.server.endpoint.DeliveryInfo;
import org.jboss.jms.server.endpoint.DeliveryRecovery;
import org.jboss.jms.tx.MessagingXAResource;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.logging.Logger;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

import javax.jms.Session;

/**
 * State corresponding to a session. This state is acessible inside aspects/interceptors.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SessionState extends HierarchicalStateSupport
{

   // Constants ------------------------------------------------------------------------------------

   protected static Logger log = Logger.getLogger(SessionState.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private ConnectionState parent;
   private SessionDelegate delegate;

   private int sessionId;
   private int acknowledgeMode;
   private boolean transacted;
   private boolean xa;

   private MessagingXAResource xaResource;
   private Object currentTxId;

   // Executor used for executing onMessage methods
   private QueuedExecutor executor;

   private boolean recoverCalled;

   // List<DeliveryInfo>
   private List clientAckList;

   // List<DeliveryInfo>
   //private List clientCancelList;
   
   private DeliveryInfo autoAckInfo;
   private Map callbackHandlers;


   // Constructors ---------------------------------------------------------------------------------

   public SessionState(ConnectionState parent, ClientSessionDelegate delegate,
                       boolean transacted, int ackMode, boolean xa)
   {
      super(parent, (DelegateSupport)delegate);

      this.sessionId = delegate.getID();

      children = new HashSet();
      this.acknowledgeMode = ackMode;
      this.transacted = transacted;
      this.xa = xa;

      if (xa)
      {
         // Create an XA resource
         xaResource = new MessagingXAResource(parent.getResourceManager(), this);
      }

      // If session is transacted and XA, the currentTxId will be updated when the XAResource will
      // be enrolled with a global transaction.

      if (transacted & !xa)
      {
         // Create a local tx
         currentTxId = parent.getResourceManager().createLocalTx();
      }

      executor = new QueuedExecutor(new LinkedQueue());

      clientAckList = new ArrayList();

     // clientCancelList = new ArrayList();
      
      // TODO could optimise this to use the same map of callbackmanagers (which holds refs
      // to callbackhandlers) in the connection, instead of maintaining another map
      callbackHandlers = new HashMap();
   }


   // HierarchicalState implementation -------------------------------------------------------------

   public DelegateSupport getDelegate()
   {
      return (DelegateSupport)delegate;
   }

   public void setDelegate(DelegateSupport delegate)
   {
      this.delegate = (SessionDelegate)delegate;
   }

   public HierarchicalState getParent()
   {
      return parent;
   }

   public void setParent(HierarchicalState parent)
   {
      this.parent = (ConnectionState)parent;
   }

   public Version getVersionToUse()
   {
      return parent.getVersionToUse();
   }

   // HierarchicalStateSupport overrides -----------------------------------------------------------

   public void synchronizeWith(HierarchicalState ns) throws Exception
   {
      SessionState newState = (SessionState)ns;

      int oldSessionID = sessionId;
      sessionId = newState.sessionId;

      ClientSessionDelegate newDelegate = (ClientSessionDelegate)newState.getDelegate();

      for (Iterator i = getChildren().iterator(); i.hasNext(); )
      {
         HierarchicalState child = (HierarchicalState)i.next();

         if (child instanceof ProducerState)
         {
            handleFailoverOnProducer((ProducerState)child, newDelegate);
         }
         else if (child instanceof ConsumerState)
         {
            handleFailoverOnConsumer((ClientConnectionDelegate)getParent().getDelegate(),
                                     (ConsumerState)child, newDelegate);
         }
         else if (child instanceof BrowserState)
         {
             handleFailoverOnBrowser((BrowserState)child, newDelegate);
         }
      }

      ConnectionState connState = (ConnectionState)getParent();
      ResourceManager rm = connState.getResourceManager();

      // We need to failover from one session ID to another in the resource manager
      rm.handleFailover(connState.getServerID(), oldSessionID, newState.sessionId);

      List ackInfos = Collections.EMPTY_LIST;

      if (!isTransacted() || (isXA() && getCurrentTxId() == null))
      {
         // Non transacted session or an XA session with no transaction set (it falls back
         // to auto_ack)

         log.debug(this + " is not transacted (or XA with no tx set), " +
            "retrieving deliveries from session state");

         // We remove any unacked non-persistent messages - this is because we don't want to ack
         // them since the server won't know about them and will get confused

         if (getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE)
         {
            for(Iterator i = getClientAckList().iterator(); i.hasNext(); )
            {
               DeliveryInfo info = (DeliveryInfo)i.next();
               if (!info.getMessageProxy().getMessage().isReliable())
               {
                  i.remove();
                  log.debug("removed non persistent delivery " + info);
               }
            }

            ackInfos = getClientAckList();
         }
         else
         {
            DeliveryInfo autoAck = getAutoAckInfo();
            if (autoAck != null)
            {
               if (!autoAck.getMessageProxy().getMessage().isReliable())
               {
                  // unreliable, discard
                  setAutoAckInfo(null);
               }
               else
               {
                  // reliable
                  ackInfos = new ArrayList();
                  ackInfos.add(autoAck);
               }
            }
         }

         log.debug(this + " retrieved " + ackInfos.size() + " deliveries");
      }
      else
      {
         // Transacted session - we need to get the acks from the resource manager. BTW we have
         // kept the old resource manager

         ackInfos = rm.getDeliveriesForSession(getSessionId());
      }

      if (!ackInfos.isEmpty())
      {
         SessionDelegate nd = (SessionDelegate)getDelegate();

         List recoveryInfos = new ArrayList();

         for (Iterator i = ackInfos.iterator(); i.hasNext(); )
         {
            DeliveryInfo info = (DeliveryInfo)i.next();

            DeliveryRecovery recInfo =
               new DeliveryRecovery(info.getMessageProxy().getDeliveryId(),
                                    info.getMessageProxy().getMessage().getMessageID(),
                                    info.getChannelId());

            recoveryInfos.add(recInfo);
         }

         log.debug(this + " sending delivery recovery info: " + recoveryInfos);
         nd.recoverDeliveries(recoveryInfos);
      }
      else
      {
         log.debug(this + " no delivery recovery info to send");
      }
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * @return List<AckInfo>
    */
   public List getClientAckList()
   {
      return clientAckList;
   }

   public DeliveryInfo getAutoAckInfo()
   {
      return autoAckInfo;
   }

   public void setAutoAckInfo(DeliveryInfo info)
   {
      if (info != null && autoAckInfo != null)
      {
         throw new IllegalStateException("There is already a delivery set for auto ack");
      }
      autoAckInfo = info;
   }

   public int getAcknowledgeMode()
   {
      return acknowledgeMode;
   }

   public boolean isTransacted()
   {
      return transacted;
   }

   public boolean isXA()
   {
      return xa;
   }

   public MessagingXAResource getXAResource()
   {
      return xaResource;
   }

   public QueuedExecutor getExecutor()
   {
      return executor;
   }

   public Object getCurrentTxId()
   {
      return currentTxId;
   }

   public boolean isRecoverCalled()
   {
      return recoverCalled;
   }

   public void setCurrentTxId(Object id)
   {
      this.currentTxId = id;
   }

   public void setRecoverCalled(boolean recoverCalled)
   {
      this.recoverCalled = recoverCalled;
   }

   public MessageCallbackHandler getCallbackHandler(int consumerID)
   {
      return (MessageCallbackHandler)callbackHandlers.get(new Integer(consumerID));
   }

   public void addCallbackHandler(MessageCallbackHandler handler)
   {
      callbackHandlers.put(new Integer(handler.getConsumerId()), handler);
   }

   public void removeCallbackHandler(MessageCallbackHandler handler)
   {
      callbackHandlers.remove(new Integer(handler.getConsumerId()));
   }

   public int getSessionId()
   {
      return sessionId;
   }

   public String toString()
   {
      return "SessionState[" + sessionId + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   /**
    * TODO See http://jira.jboss.org/jira/browse/JBMESSAGING-708
    */
   private void handleFailoverOnConsumer(ClientConnectionDelegate failedConnectionDelegate,
                                         ConsumerState failedConsumerState,
                                         ClientSessionDelegate newSessionDelegate)

      throws Exception
   {
      log.debug(this + " failing over consumer " + failedConsumerState);

      CallbackManager oldCallbackManager =
         failedConnectionDelegate.getRemotingConnection().getCallbackManager();

      ClientConsumerDelegate failedConsumerDelegate =
         (ClientConsumerDelegate)failedConsumerState.getDelegate();

      log.debug(this + " creating alternate consumer");

      ClientConsumerDelegate newConsumerDelegate = (ClientConsumerDelegate)newSessionDelegate.
         createConsumerDelegate((JBossDestination)failedConsumerState.getDestination(),
                                failedConsumerState.getSelector(),
                                failedConsumerState.isNoLocal(),
                                failedConsumerState.getSubscriptionName(),
                                failedConsumerState.isConnectionConsumer(),
                                failedConsumerState.getChannelId());

      log.debug(this + " alternate consumer created");

      // Copy the attributes from the new consumer to the old consumer
      failedConsumerDelegate.synchronizeWith(newConsumerDelegate);

      ConsumerState newState = (ConsumerState)newConsumerDelegate.getState();

      int oldConsumerID = failedConsumerState.getConsumerID();

      // Update attributes on the old state
      failedConsumerState.copyState(newState);

      // We need to re-use the existing message callback handler

      MessageCallbackHandler oldHandler =
         oldCallbackManager.unregisterHandler(oldConsumerID);

      ConnectionState newConnectionState = (ConnectionState)failedConnectionDelegate.getState();

      CallbackManager newCallbackManager =
         newConnectionState.getRemotingConnection().getCallbackManager();

      // Remove the new handler
      MessageCallbackHandler newHandler = newCallbackManager.
         unregisterHandler(newState.getConsumerID());

      log.debug("New handler is " + System.identityHashCode(newHandler));

      //But we need to update some fields from the new one
      oldHandler.copyState(newHandler);

      //Now we re-register the old handler with the new callback manager

      newCallbackManager.registerHandler(newState.getConsumerID(),
                                         oldHandler);

      // We don't need to add the handler to the session state since it is already there - we
      // are re-using the old handler

      log.debug(this + " failed over consumer");
   }


   /**
    * TODO see http://jira.jboss.org/jira/browse/JBMESSAGING-709
    */
   private void handleFailoverOnProducer(ProducerState failedProducerState,
                                         ClientSessionDelegate failedSessionDelegate)
      throws Exception
   {
      ClientProducerDelegate newProducerDelegate = (ClientProducerDelegate)failedSessionDelegate.
         createProducerDelegate((JBossDestination)failedProducerState.getDestination());

      ClientProducerDelegate failedProducerDelegate =
         (ClientProducerDelegate)failedProducerState.getDelegate();

      failedProducerDelegate.synchronizeWith(newProducerDelegate);
      failedProducerState.copyState((ProducerState)newProducerDelegate.getState());

      log.debug("handling fail over on producerDelegate " + failedProducerDelegate + " destination=" + failedProducerState.getDestination());
   }


   /**
    * TODO see http://jira.jboss.org/jira/browse/JBMESSAGING-710
    */
   private void handleFailoverOnBrowser(BrowserState failedBrowserState,
                                        ClientSessionDelegate failedSessionDelegate)
      throws Exception
   {
      ClientBrowserDelegate newBrowserDelegate = (ClientBrowserDelegate)failedSessionDelegate.
         createBrowserDelegate(failedBrowserState.getJmsDestination(),
                               failedBrowserState.getMessageSelector());

      ClientBrowserDelegate failedBrowserDelegate =
         (ClientBrowserDelegate)failedBrowserState.getDelegate();

      failedBrowserDelegate.synchronizeWith(newBrowserDelegate);
      failedBrowserState.copyState((BrowserState)newBrowserDelegate.getState());

      log.debug("handling fail over on browserDelegate " + failedBrowserDelegate + " destination=" + failedBrowserState.getJmsDestination() + " selector=" + failedBrowserState.getMessageSelector());

   }


   // Inner classes --------------------------------------------------------------------------------



}


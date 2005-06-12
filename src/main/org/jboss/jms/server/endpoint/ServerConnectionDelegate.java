/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;

import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.transaction.TransactionManager;

import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.aop.util.PayloadKey;
import org.jboss.jms.client.container.InvokerInterceptor;
import org.jboss.jms.client.container.JMSInvocationHandler;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.DurableSubscriptionHolder;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.TxInfo;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.AbstractDestination;
import org.jboss.util.id.GUID;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerConnectionDelegate implements ConnectionDelegate
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ServerConnectionDelegate.class);
   
   // Static --------------------------------------------------------
   
   
   
   // Attributes ----------------------------------------------------
   
   private int sessionIDCounter;
   
   protected String connectionID;
   
   protected ServerPeer serverPeer;
   
   protected Map sessions;
   
   protected Set temporaryDestinations;
   
   protected volatile boolean started;
   
   protected String clientID;
   
   protected Map receivers;
   
    
   // Constructors --------------------------------------------------
   
   public ServerConnectionDelegate(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
      sessionIDCounter = 0;
      sessions = new HashMap();
      temporaryDestinations = new HashSet();
      started = false;
      connectionID = new GUID().toString();
      receivers = new ConcurrentReaderHashMap();
   }
   
   // ConnectionDelegate implementation -----------------------------
   
   public SessionDelegate createSessionDelegate(boolean transacted, int acknowledgmentMode)
   {
      // create the dynamic proxy that implements SessionDelegate
      SessionDelegate sd = null;
      Serializable oid = serverPeer.getSessionAdvisor().getName();
      String stackName = "SessionStack";
      AdviceStack stack = AspectManager.instance().getAdviceStack(stackName);
      
      // TODO why do I need to the advisor to create the interceptor stack?
      Interceptor[] interceptors = stack.createInterceptors(serverPeer.getSessionAdvisor(), null);
      
      // TODO: The ConnectionFactoryDelegate and ConnectionDelegate share the same locator (TCP/IP connection?). Performance?
      JMSInvocationHandler h = new JMSInvocationHandler(interceptors);
      
      String sessionID = generateSessionID();
      
      SimpleMetaData metadata = new SimpleMetaData();
      // TODO: The ConnectionFactoryDelegate and ConnectionDelegate share the same locator (TCP/IP connection?). Performance?
      metadata.addMetaData(Dispatcher.DISPATCHER, Dispatcher.OID, oid, PayloadKey.AS_IS);
      metadata.addMetaData(InvokerInterceptor.REMOTING,
            InvokerInterceptor.INVOKER_LOCATOR,
            serverPeer.getLocator(),
            PayloadKey.AS_IS);
      metadata.addMetaData(InvokerInterceptor.REMOTING,
            InvokerInterceptor.SUBSYSTEM,
            "JMS",
            PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CONNECTION_ID, connectionID, PayloadKey.AS_IS);
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.SESSION_ID, sessionID, PayloadKey.AS_IS);
      
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.ACKNOWLEDGMENT_MODE,
            new Integer(acknowledgmentMode), PayloadKey.AS_IS);
      
      
      h.getMetaData().mergeIn(metadata);
      
      // TODO 
      ClassLoader loader = getClass().getClassLoader();
      Class[] interfaces = new Class[] { SessionDelegate.class };
      sd = (SessionDelegate)Proxy.newProxyInstance(loader, interfaces, h);      
      
      // create the corresponding "server-side" SessionDelegate and register it with this
      // ConnectionDelegate instance
      ServerSessionDelegate ssd = new ServerSessionDelegate(sessionID, this, acknowledgmentMode);
      putSessionDelegate(sessionID, ssd);
      
      log.debug("created session delegate (sessionID=" + sessionID + ")");
      
      return sd;
   }
   
   
   
   public String getClientID() throws JMSException
   {
      throw new JMSException("getClientID() is not handled on the server");
   }
   
   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }
   
   
   public void start()
   {
      setStarted(true);
      log.debug("Connection " + connectionID + " started");
   }
   
   public boolean isStarted()
   {
      return started;
   }
   
   public synchronized void stop()
   {
      setStarted(false);
      log.debug("Connection " + connectionID + " stopped");
   }
   
   public void close() throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("In ServerConnectionDelegate.close()"); }
      
      DestinationManager dm = serverPeer.getDestinationManager();
      Iterator iter = this.temporaryDestinations.iterator();
      while (iter.hasNext())
      {
         JBossDestination dest = (JBossDestination)iter.next();
         dm.removeDestination(dest.getName());
      }
      this.temporaryDestinations = null;
      this.receivers = null;
      
      //TODO do we need to traverse the sessions and close them explicitly here?
      //Or do we rely on the ClosedInterceptor to do this?
   }
   
   public void closing() throws JMSException
   {
      log.trace("In ServerConnectionDelegate.closing()");
      
      //This currently does nothing
   }
   
   public ExceptionListener getExceptionListener() throws JMSException
   {
      throw new IllegalStateException("getExceptionListener is not handled on the server");
   }
   
   public void setExceptionListener(ExceptionListener listener) throws JMSException
   {
      throw new IllegalStateException("setExceptionListener is not handled on the server");
   }
   
   
   public void sendTransaction(TxInfo tx) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("Received transaction from client"); }
      if (log.isTraceEnabled()) { log.trace("There are " + tx.getMessages().size() + " messages to send " +	"and " + tx.getAcks().size() + " messages to ack"); }
      
      TransactionManager tm = serverPeer.getTransactionManager();
      boolean committed = false;
      
      try
      {
         // start the transaction
         tm.begin();
         
         Iterator iter = tx.getMessages().iterator();
         while (iter.hasNext())
         {
            Message m = (Message)iter.next();
            sendMessage(m);
            if (log.isTraceEnabled()) { log.trace("Sent message"); }
         }
         
         if (log.isTraceEnabled()) { log.trace("Done the sends"); }
         
         //Then ack the acks
         iter = tx.getAcks().iterator();
         while (iter.hasNext())
         {
            AckInfo ack = (AckInfo)iter.next();
            
            acknowledge(ack.messageID, ack.receiverID);
            
            if (log.isTraceEnabled()) { log.trace("Acked message:" + ack.messageID); }
         }
         
         if (log.isTraceEnabled()) { log.trace("Done the acks"); }
         
         tm.commit();
         committed = true;
      }
      catch (Throwable t)
      {
         String msg = "The server connection delegate failed to handle transaction";
         log.error(msg, t);
         throw new JBossJMSException(msg, t);
      }
      finally
      {
         if (tm != null && !committed)
         {
            try
            {
               tm.rollback();
            }
            catch(Exception e)
            {
               log.debug("Unable to rollback curent non-committed transaction", e);
            }
         }
      }
      
   }
   
   public void addTemporaryDestination(Destination dest) throws JMSException
   {
      JBossDestination d = (JBossDestination)dest;
      if (!d.isTemporary())
      {
         throw new JMSException("Destination:" + dest + " is not a temporary destination");
      }
      this.temporaryDestinations.add(dest);
      this.serverPeer.getDestinationManager().addDestination(dest);
   }
   
   public void deleteTemporaryDestination(Destination dest) throws JMSException
   {
      JBossDestination d = (JBossDestination)dest;
      
      if (!d.isTemporary())
      {
         throw new JMSException("Destination:" + dest + " is not a temporary destination");
      }
      
      DestinationManager dm = serverPeer.getDestinationManager();
      dm.removeDestination(d.getName());
     
      this.temporaryDestinations.remove(dest);
   }
   
   public void unsubscribe(String subscriptionName) throws JMSException
   {
      DurableSubscriptionHolder subscription =
         this.serverPeer.getClientManager().removeDurableSubscription(this.clientID, subscriptionName);
      if (subscription == null)
      {
         throw new JMSException("Cannot find durable subscription with name " +
            subscriptionName + " to unsubscribe");
      }
      subscription.getTopic().remove(subscription.getQueue().getReceiverID());
      
   }
   

   public Queue createQueue(String queueName, boolean create) throws JMSException
   {
      DestinationManager dm = this.serverPeer.getDestinationManager();
   
      AbstractDestination dest = dm.getDestination(queueName);
      
      Queue queue = null;
      
      if (dest == null)
      {
         if (!create)
         {
            throw new JMSException("There is no administratively defined queue with name:" + queueName);
         }
         else
         {
            queue = new JBossQueue(queueName);
            dm.addDestination(queue);
         }
      }
      else
      {
         queue = new JBossQueue(queueName);
      }
      return queue;  
   }
   

   public Topic createTopic(String topicName, boolean create) throws JMSException
   {
      DestinationManager dm = this.serverPeer.getDestinationManager();
      
      AbstractDestination dest = dm.getDestination(topicName);
      
      Topic topic = null;
      
      if (dest == null)
      {
         if (!create)
         {
            throw new JMSException("There is no administratively defined topic with name:" + topicName);
         }
         else
         {
            topic = new JBossTopic(topicName);
            dm.addDestination(topic);
         }
      }
      else
      {
         topic = new JBossTopic(topicName);
      }
      return topic;  
   }
  
   
   // Public --------------------------------------------------------
   
   public ServerSessionDelegate putSessionDelegate(String sessionID, ServerSessionDelegate d)
   {
      synchronized(sessions)
      {
         return (ServerSessionDelegate)sessions.put(sessionID, d);
      }
   }
   
   public ServerSessionDelegate getSessionDelegate(String sessionID)
   {
      synchronized(sessions)
      {
         return (ServerSessionDelegate)sessions.get(sessionID);
      }
   }
   
   public ServerPeer getServerPeer()
   {
      return serverPeer;
   }
   
   
   public String getConnectionID()
   {
      return connectionID;
   }
   
   
   // Package protected ---------------------------------------------
   
   void sendMessage(Message m) throws JMSException
   {
      //The JMSDestination header must already have been set for each message
      JBossDestination dest = (JBossDestination)m.getJMSDestination();
      if (dest == null)
      {
         throw new IllegalStateException("JMSDestination header not set!");
      }
    
      AbstractDestination destination = null;

      DestinationManager dm = serverPeer.getDestinationManager();
      destination = dm.getDestination(dest);
      
      if (destination == null)
      {
         throw new JMSException("Destination " + dest.getName() + " does not exist");
      }
      
      m.setJMSMessageID(generateMessageID());
      
      //This allows the no-local consumers to filter out the messages that come from the
      //same connection
      //TODO Do we want to set this for ALL messages. Possibly an optimisation is possible here
      ((JBossMessage)m).setConnectionID(connectionID);
      
      boolean acked = destination.handle((Routable)m);
      
      if (destination.isTransactional() && isActiveTransaction())
      {
         // for a transacted invocation, the return value is irrelevant
         return;
      }
      
      if (!acked)
      {
         // under normal circumstances, this shouldn't happen, since the destination
         // is supposed to hold the message for redelivery
         
         String msg = "The message was not acknowledged by destination " + destination;
         log.error(msg);
         throw new JBossJMSException(msg);
      }
   }
   
   void acknowledge(String messageID, String receiverID)
      throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("receiving ACK for " + messageID); }
           
      ServerConsumerDelegate receiver = (ServerConsumerDelegate)receivers.get(receiverID);
      if (receiver == null)
      {
         throw new IllegalStateException("Cannot find receiver:" + receiverID);
      }
      
      receiver.acknowledge(messageID);
    
   }
   

   // Protected -----------------------------------------------------
   
   /**
    * Generates a sessionID that is unique per this ConnectionDelegate instance
    */
   protected String generateSessionID()
   {
      int id;
      synchronized(this)
      {
         id = sessionIDCounter++;
      }
      return connectionID + "-Session" + id;
   }
   
   protected String generateMessageID()
   {
      StringBuffer sb = new StringBuffer("ID:");
      sb.append(new GUID().toString());
      return sb.toString();
   }
   
   // Private -------------------------------------------------------
   
   private void setStarted(boolean s)
   {
      synchronized(sessions)
      {
         for (Iterator i = sessions.values().iterator(); i.hasNext(); )
         {
            ServerSessionDelegate sd = (ServerSessionDelegate)i.next();
            sd.setStarted(s);
         }
         started = s;
      }
   }
   
   private boolean isActiveTransaction()
   {
      TransactionManager tm = serverPeer.getTransactionManager();
      if (tm == null)
      {
         return false;
      }
      try
      {
         return tm.getTransaction() != null;
      }
      catch(Exception e)
      {
         log.debug("failed to access transaction manager", e);
         return false;
      }
   }
   
   
   // Inner classes -------------------------------------------------
}

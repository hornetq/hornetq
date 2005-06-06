/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.endpoint;

import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.jms.tx.AckInfo;
import org.jboss.jms.tx.TxInfo;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.jms.client.container.JMSInvocationHandler;
import org.jboss.jms.client.container.InvokerInterceptor;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.util.PayloadKey;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.AbstractDestination;
import org.jboss.util.id.GUID;

import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.transaction.TransactionManager;

import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.io.Serializable;
import java.lang.reflect.Proxy;

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
   
   protected String clientID;
   protected ServerPeer serverPeer;
   
   protected Map sessions;
   
   protected Set temporaryDestinations;
   
   protected volatile boolean started;
   
   // Constructors --------------------------------------------------
   
   public ServerConnectionDelegate(String clientID, ServerPeer serverPeer)
   {
      this.clientID = clientID;
      this.serverPeer = serverPeer;
      sessionIDCounter = 0;
      sessions = new HashMap();
      temporaryDestinations = new HashSet();
      started = false;
      
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
      metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.CLIENT_ID, clientID, PayloadKey.AS_IS);
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
   
   public String getClientID()
   {
      return clientID;
   }
   
   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }
   
   public void start()
   {
      setStarted(true);
      log.debug("Connection " + clientID + " started");
   }
   
   public boolean isStarted()
   {
      return started;
   }
   
   public synchronized void stop()
   {
      setStarted(false);
      log.debug("Connection " + clientID + " stopped");
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
            if (log.isTraceEnabled()) log.trace("Sent message");
         }
         
         if (log.isTraceEnabled()) log.trace("Done the sends");
         
         //Then ack the acks
         iter = tx.getAcks().iterator();
         while (iter.hasNext())
         {
            AckInfo ack = (AckInfo)iter.next();
            
            acknowledge(ack.messageID, ack.destination, ack.receiverID);
            
            if (log.isTraceEnabled()) log.trace("Acked message:" + ack.messageID);
         }
         
         if (log.isTraceEnabled()) log.trace("Done the acks");
         
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
   
   void acknowledge(String messageID, Destination jmsDestination, String receiverID)
      throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("receiving ACK for " + messageID); }
      
      DestinationManager dm = serverPeer.getDestinationManager();
      
      AbstractDestination destination =  dm.getDestination(jmsDestination);
      
      destination.acknowledge(messageID, receiverID);      
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
      return clientID + "-Session" + id;
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

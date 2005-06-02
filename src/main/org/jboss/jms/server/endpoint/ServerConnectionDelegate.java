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
import org.jboss.aop.advice.AdviceStack;
import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.AspectManager;
import org.jboss.aop.Dispatcher;
import org.jboss.aop.util.PayloadKey;
import org.jboss.aop.metadata.SimpleMetaData;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.AbstractDestination;
import org.jboss.messaging.core.util.transaction.TransactionManagerImpl;
import org.jboss.util.id.GUID;

import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.transaction.TransactionManager;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
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
   protected volatile boolean started;

   // Constructors --------------------------------------------------

   public ServerConnectionDelegate(String clientID, ServerPeer serverPeer)
   {
      this.clientID = clientID;
      this.serverPeer = serverPeer;
      sessionIDCounter = 0;
      sessions = new HashMap();
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
      
		metadata.addMetaData(JMSAdvisor.JMS, JMSAdvisor.ACKNOWLEDGMENT_MODE, new Integer(acknowledgmentMode), PayloadKey.AS_IS);
		

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
      
      /* I don't think we need to close the children here since
      The traversal of the children is done in the ClosedInterceptor */                                                 
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
		if (log.isTraceEnabled()) log.trace("Received transaction from client");
		
		if (log.isTraceEnabled()) log.trace("There are " + tx.getMessages().size() + " messages to send " +
				"and " + tx.getAcks().size() + " messages to ack");
                     
		TransactionManager tm = TransactionManagerImpl.getInstance();		
		
		/*
		
		try
		{
			tm.begin();
		}
		catch (Exception e)
		{
			throw new JMSException("Failed to start transaction", e.getMessage());
		}
		
		*/
				
		try
		{		
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
			
		}
		catch (Exception e)
		{
			//TODO What to do here??
		}
		
		/*
		try
		{
			tm.commit();
		}
		catch (Exception e)
		{
			log.error("Failed to commit tx", e);
			throw new JMSException("Failed to commit transaction", e.getMessage());
		}
		*/
			
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
      Destination dest = m.getJMSDestination();
      if (dest == null)
      {
         throw new IllegalStateException("JMSDestination header not set!");
      }
      
      Receiver receiver = null;
      try
      {
         DestinationManager dm = serverPeer.getDestinationManager();
         receiver = dm.getDestination(dest);
      }
      catch(Exception e)
      {
         throw new JBossJMSException("Cannot map destination " + dest, e);
      }
      
      m.setJMSMessageID(generateMessageID());         
   
      boolean acked = receiver.handle((Routable)m);
   
      if (!acked)
      {
         log.debug("The message was not acknowledged");
         //TODO deal with this properly
      }  
   }
	
	void acknowledge(String messageID, Destination jmsDestination, String receiverID)
		throws JMSException
	{
		 if (log.isTraceEnabled()) { log.trace("receiving ACK for " + messageID); }
			
		 DestinationManager dm = serverPeer.getDestinationManager();
			
		 AbstractDestination destination = null;
		 try
		 {
			 destination = dm.getDestination(jmsDestination);
		 }
		 catch(Exception e)
		 {
			 throw new JBossJMSException("Cannot map destination " + jmsDestination, e);
		 }
		 
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


   // Inner classes -------------------------------------------------
}

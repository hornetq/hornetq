/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;

import org.jboss.jms.server.endpoint.ServerConnectionDelegate;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.DurableSubscription;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * Manages client connections. There is a single ClientManager instance for each server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientManager
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ClientManager.class);


   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;
   protected Map connections;
   protected Map subscriptions;
   
   protected ConnectionChecker checker;
   
   protected long pingCheckInterval;
   
   protected long connectionCloseTime;

   // Constructors --------------------------------------------------

   public ClientManager(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
      connections = new ConcurrentReaderHashMap();
      subscriptions = new ConcurrentReaderHashMap();
      //checker = new ConnectionChecker();
      //checker.start();
      
      //default values
      pingCheckInterval = 4000;
      connectionCloseTime = 10000;
   }

   // Public --------------------------------------------------------
   
   public void stop()
   {
      //checker.stop();
   }

   public ServerConnectionDelegate putConnectionDelegate(Serializable connectionID,
                                                         ServerConnectionDelegate d)
   {
      return (ServerConnectionDelegate)connections.put(connectionID, d);
   }
   
   public void removeConnectionDelegate(Serializable connectionID)
   {
      connections.remove(connectionID);
   }

   public ServerConnectionDelegate getConnectionDelegate(Serializable connectionID)
   {
      return (ServerConnectionDelegate)connections.get(connectionID);
   }
   
   public Set getDurableSubscriptions(String clientID)
   {
      Map subs = (Map)subscriptions.get(clientID);
      return subs != null ? new HashSet(subs.values()) : Collections.EMPTY_SET;
   }
   
   public DurableSubscription getDurableSubscription(String clientID, String subscriptionName)
   {
      Map subs = (Map)subscriptions.get(clientID);
      return subs == null ? null : (DurableSubscription)subs.get(subscriptionName);
   }
   
   public void addDurableSubscription(String clientID, String subscriptionName,
         DurableSubscription subscription)
   {
      Map subs = (Map)subscriptions.get(clientID);
      if (subs == null)
      {
         subs = new ConcurrentReaderHashMap();
         subscriptions.put(clientID, subs);
      }
      subs.put(subscriptionName, subscription);
   }
   
   public DurableSubscription removeDurableSubscription(String clientID,
                                                              String subscriptionName)
      throws JMSException
   {
      if (clientID == null)
      {
         throw new JMSException("Client ID must be set for connection!");
      }
      
      Map subs = (Map)subscriptions.get(clientID);
      
      if (subs == null)
      {
         return null;
      }
                 
      
      if (log.isTraceEnabled()) { log.trace("Removing durable sub: " + subscriptionName); }
      
      DurableSubscription removed = (DurableSubscription)subs.remove(subscriptionName);
      
      if (subs.size() == 0)
      {
         subscriptions.remove(clientID);
      }
      
      return removed;
   }
   
   public long getConnectionCloseTime()
   {
      return connectionCloseTime;
   }
   
   public long getPingCheckInterval()
   {
      return pingCheckInterval;
   }
   
   public void setConnectionCloseTime(long time)
   {
      this.connectionCloseTime = time;
   }
   
   public void setPingCheckInterval(long time)
   {
      this.pingCheckInterval = time;
   }
   

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   class ConnectionChecker implements Runnable
   {
      //TODO Needs to be configurable
      protected boolean stopping;
      
      protected Thread checkerThread;
      
      protected ClientManager clientManager;
      
      public ConnectionChecker()
      {
      }
      
      private synchronized void setStopping()
      {
         stopping = true;
      }
      
      private synchronized boolean isStopping()
      {
         return stopping;
      }
      
      public void start()
      {
         if (log.isTraceEnabled()) { log.trace("Starting connection checker"); }
         checkerThread = new Thread(this);
         checkerThread.setDaemon(true);
         checkerThread.start();
         if (log.isTraceEnabled()) { log.trace("Connection checker started"); }
      }
      
      public void stop()
      {
         if (log.isTraceEnabled()) { log.trace("Stopping connection checker"); }
         setStopping();
         try
         {
            checkerThread.interrupt();
            checkerThread.join();
         }
         catch (InterruptedException e)
         {
            log.error("Connection checker thread interrupted");
         }
         if (log.isTraceEnabled()) { log.trace("Connection checker stopped"); }
      }
      
      public void run()
      {
         while (!isStopping())
         {
            try
            {
               Thread.sleep(pingCheckInterval);
            }
            catch (InterruptedException e)
            {
               //Ignore
               break;
            }
            
            long now = System.currentTimeMillis();
            Iterator iter = connections.values().iterator();
            while (iter.hasNext())
            {
               ServerConnectionDelegate conn = (ServerConnectionDelegate)iter.next();
               
               //long lastPinged = conn.getLastPinged();
               long lastPinged = 0;
               
               if (now - lastPinged > connectionCloseTime)
               {
                  log.warn("Connection: " + conn.getConnectionID() + " has not pinged me for " + (now - lastPinged) + " ms, so I am closing it.");
                  //Close the connection
                  try
                  {
                     conn.close();
                  }
                  catch (JMSException e)
                  {
                     log.error("Failed to close connection", e);
                  }
               }
               
            }
            
         }
      } 
   }
}

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
package org.jboss.jms.server.container;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSSecurityException;
import javax.jms.Message;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.SecurityManager;
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.jms.server.endpoint.ServerConsumerEndpoint;
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.jms.server.endpoint.advised.ConnectionAdvised;
import org.jboss.jms.server.endpoint.advised.ConsumerAdvised;
import org.jboss.jms.server.endpoint.advised.SessionAdvised;
import org.jboss.jms.server.security.SecurityMetadata;
import org.jboss.jms.tx.ClientTransaction;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.logging.Logger;
import org.jboss.security.SecurityAssociation;

/**
 * This aspect enforces the JBossMessaging JMS security policy.
 * 
 * This aspect is PER_INSTANCE
 * 
 * For performance reasons we cache access rights in the interceptor for a maximum of
 * INVALIDATION_INTERVAL milliseconds.
 * This is because we don't want to do a full authentication and authorization on every send,
 * for example, since this will drastically reduce performance.
 * This means any changes to security data won't be reflected until INVALIDATION_INTERVAL
 * milliseconds later.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision 1.1 $</tt>
 *
 * $Id$
 */
public class SecurityAspect
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SecurityAspect.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   private Set readCache;
   
   private Set writeCache;
   
   private Set createCache;
   
   //TODO Make this configurable
   private static final long INVALIDATION_INTERVAL = 15000;
   
   private long lastCheck;
      
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   public SecurityAspect()
   {
      readCache = new HashSet();
      
      writeCache = new HashSet();
      
      createCache = new HashSet();
   }
   
   public Object handleCreateConsumerDelegate(Invocation invocation) throws Throwable
   {
      MethodInvocation mi = (MethodInvocation)invocation;
      
      // read permission required on the destination
      Destination dest = (Destination)mi.getArguments()[0];
      
      SessionAdvised del = (SessionAdvised)invocation.getTargetObject();
      ServerSessionEndpoint sess = (ServerSessionEndpoint)del.getEndpoint();
      
      check(dest, CheckType.READ, sess.getConnectionEndpoint());
      
      // if creating a durable subscription then need create permission
      
      String subscriptionName = (String)mi.getArguments()[3];
      if (subscriptionName != null)
      {
         // durable
         check(dest, CheckType.CREATE, sess.getConnectionEndpoint());
      }
      
      return invocation.invokeNext();
   }   
   
   public Object handleCreateBrowserDelegate(Invocation invocation) throws Throwable
   {
      // read permission required on the destination
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Destination dest = (Destination)mi.getArguments()[0];
      
      SessionAdvised del = (SessionAdvised)invocation.getTargetObject();
      ServerSessionEndpoint sess = (ServerSessionEndpoint)del.getEndpoint();
                  
      check(dest, CheckType.READ, sess.getConnectionEndpoint());
      
      return invocation.invokeNext();
   }
   
   public Object handleSend(Invocation invocation) throws Throwable
   {
      // anonymous producer - if destination is not null then write permissions required
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Message m = (Message)mi.getArguments()[0];
      Destination dest = m.getJMSDestination();

      SessionAdvised del = (SessionAdvised)invocation.getTargetObject();
      ServerSessionEndpoint se = (ServerSessionEndpoint)del.getEndpoint();
      ServerConnectionEndpoint ce = se.getConnectionEndpoint();
                        
      check(dest, CheckType.WRITE, ce);
            
      return invocation.invokeNext();
   }


   // An aspect over ConnectionAdvised
   public Object handleSendTransaction(Invocation invocation) throws Throwable
   {
      ConnectionAdvised del = (ConnectionAdvised)invocation.getTargetObject();
      ServerConnectionEndpoint ce = (ServerConnectionEndpoint)del.getEndpoint();

      MethodInvocation mi = (MethodInvocation)invocation;

      TransactionRequest t = (TransactionRequest)mi.getArguments()[0];

      ClientTransaction txState = t.getState();

      if (txState != null)
      {
         // distinct list of destinations...
         HashSet destinations = new HashSet();

         for (Iterator i = txState.getSessionStates().iterator(); i.hasNext(); )
         {
            ClientTransaction.SessionTxState sessionState = (ClientTransaction.SessionTxState)i.next();
            for (Iterator j = sessionState.getMsgs().iterator(); j.hasNext(); )
            {
               JBossMessage message = (JBossMessage)j.next();
               destinations.add(message.getJMSDestination());
            }
         }
         for (Iterator iterDestinations = destinations.iterator();iterDestinations.hasNext();)
         {
            Destination destination = (Destination) iterDestinations.next();
            check(destination, CheckType.WRITE, ce);
         }

      }

      return invocation.invokeNext();
   }


   
   protected void checkConsumerAccess(Invocation invocation) throws Throwable
   {
      ConsumerAdvised del = (ConsumerAdvised)invocation.getTargetObject();
      ServerConsumerEndpoint cons = (ServerConsumerEndpoint)del.getEndpoint();
      ServerConnectionEndpoint conn = cons.getSessionEndpoint().getConnectionEndpoint();
      JBossDestination dest = cons.getDestination();
      
      check(dest, CheckType.READ, conn);
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
         
   private boolean checkCached(Destination dest, CheckType checkType)
   {
      long now = System.currentTimeMillis();
      
      boolean granted = false;
      
      if (now - lastCheck > INVALIDATION_INTERVAL)
      {
         readCache.clear();
         
         writeCache.clear();
         
         createCache.clear();         
      }
      else
      {         
         switch (checkType.type)
         {
            case CheckType.TYPE_READ:
            {
               granted = readCache.contains(dest);
               break;
            }
            case CheckType.TYPE_WRITE:
            {
               granted = writeCache.contains(dest);
               break;
            }
            case CheckType.TYPE_CREATE:
            {
               granted = createCache.contains(dest);
               break;
            }
            default:
            {
               throw new IllegalArgumentException("Invalid checkType:" + checkType);
            }
         }
      }
      
      lastCheck = now;
      
      return granted;
   }
   
   private void check(Destination dest, CheckType checkType, ServerConnectionEndpoint conn)
      throws JMSSecurityException
   {
      JBossDestination jbd = (JBossDestination)dest;

      if (jbd.isTemporary())
      {
         if (trace) { log.trace("skipping permission check on temporary destination " + dest); }
         return;
      }

      if (trace) { log.trace("checking access permissions to " + dest); }
      
      if (checkCached(dest, checkType))
      {
         // OK
         return;
      }

      boolean isQueue = jbd.isQueue();
      String name = jbd.getName();

      SecurityManager sm = conn.getSecurityManager();
      SecurityMetadata securityMetadata = sm.getSecurityMetadata(isQueue, name);

      if (securityMetadata == null)
      {
         throw new JMSSecurityException("No security configuration avaliable for " + name);
      }

      // Authenticate. Successful autentication will place a new SubjectContext on thread local,
      // which will be used in the authorization process. However, we need to make sure we clean up
      // thread local immediately after we used the information, otherwise some other people
      // security my be screwed up, on account of thread local security stack being corrupted.

      sm.authenticate(conn.getUsername(), conn.getPassword());

      // Authorize
      Set principals = checkType == CheckType.READ ? securityMetadata.getReadPrincipals() :
                       checkType == CheckType.WRITE ? securityMetadata.getWritePrincipals() :
                       securityMetadata.getCreatePrincipals();
      try
      {
         if (!sm.authorize(conn.getUsername(), principals))
         {
            String msg = "User: " + conn.getUsername() +
               " is not authorized to " +
               (checkType == CheckType.READ ? "read from" :
                  checkType == CheckType.WRITE ? "write to" : "create durable sub on") +
               " destination " + name;

            throw new JMSSecurityException(msg);
         }
      }
      finally
      {
         // pop the Messaging SecurityContext, it did its job
         SecurityAssociation.popSubjectContext();
      }

      // if we get here we're granted, add to the cache
      
      switch (checkType.type)
      {
         case CheckType.TYPE_READ:
         {
            readCache.add(dest);
            break;
         }
         case CheckType.TYPE_WRITE:
         {
            writeCache.add(dest);
            break;
         }
         case CheckType.TYPE_CREATE:
         {
            createCache.add(dest);
            break;
         }
         default:
         {
            throw new IllegalArgumentException("Invalid checkType:" + checkType);
         }
      }      
   }
   
   // Inner classes -------------------------------------------------
   
   private static class CheckType
   {
      private int type;
      private CheckType(int type)
      {
         this.type = type;
      }      
      public static final int TYPE_READ = 0;
      public static final int TYPE_WRITE = 1;
      public static final int TYPE_CREATE = 2;
      public static CheckType READ = new CheckType(TYPE_READ);
      public static CheckType WRITE = new CheckType(TYPE_WRITE);
      public static CheckType CREATE = new CheckType(TYPE_CREATE);      
      public boolean equals(Object other)
      {
         if (!(other instanceof CheckType)) return false;
         CheckType ct = (CheckType)other;
         return ct.type == this.type;
      }
      public int hashCode() 
      {
         return type;
      }
   }
}





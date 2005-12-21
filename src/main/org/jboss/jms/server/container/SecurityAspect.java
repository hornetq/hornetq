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

import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSSecurityException;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.jms.server.endpoint.ServerProducerEndpoint;
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.jms.server.endpoint.advised.SessionAdvised;
import org.jboss.jms.server.endpoint.advised.ProducerAdvised;
import org.jboss.jms.server.endpoint.advised.SessionAdvised;
import org.jboss.jms.server.security.SecurityManager;
import org.jboss.jms.server.security.SecurityMetadata;
import org.jboss.logging.Logger;

/**
 * This aspect enforces the JBossMessaging JMS security policy
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 */
public class SecurityAspect
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SecurityAspect.class);
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   public Object handleCreateConsumerDelegate(Invocation invocation) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("Checking if user has permissions to create consumer"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      //read permission required on the destination
      Destination dest = (Destination)mi.getArguments()[0];
      
      SessionAdvised del = (SessionAdvised)invocation.getTargetObject();
      ServerSessionEndpoint sess = (ServerSessionEndpoint)del.getEndpoint();
      
      check(mi, dest, CheckType.READ, sess.getConnectionEndpoint());
      
      //if creating a durable subscription then need create permission
      
      String subscriptionName = (String)mi.getArguments()[3];
      if (subscriptionName != null)
      {
         //durable
         check(mi, dest, CheckType.CREATE, sess.getConnectionEndpoint());
      }
      
      return invocation.invokeNext();
   }
   
   public Object handleCreateProducerDelegate(Invocation invocation) throws Throwable
   {
      //write permission required on the destination
      
      //Null represents an anonymous producer - the destination
      //for this is specified at send time
      
      if (log.isTraceEnabled()) { log.trace("Checking if user has permissions to create producer"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Destination dest = (Destination)mi.getArguments()[0];
      
      if (dest != null)
      {               
         SessionAdvised del = (SessionAdvised)invocation.getTargetObject();
         ServerSessionEndpoint sess = (ServerSessionEndpoint)del.getEndpoint();
                        
         check(mi, dest, CheckType.WRITE, sess.getConnectionEndpoint());
      }
      
      return invocation.invokeNext();
   }
   
   public Object handleCreateBrowserDelegate(Invocation invocation) throws Throwable
   {
      //read permission required on the destination
      
      if (log.isTraceEnabled()) { log.trace("Checking if user has permissions to create browser"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Destination dest = (Destination)mi.getArguments()[0];
      
      SessionAdvised del = (SessionAdvised)invocation.getTargetObject();
      ServerSessionEndpoint sess = (ServerSessionEndpoint)del.getEndpoint();
                  
      check(mi, dest, CheckType.READ, sess.getConnectionEndpoint());
      
      return invocation.invokeNext();
   }
   
   public Object handleSend(Invocation invocation) throws Throwable
   {
      //anonymous producer
      
      //if destination is not null then write permissions required
      
      if (log.isTraceEnabled()) { log.trace("Checking if user has permissions to send on anon. producer"); }
      
      MethodInvocation mi = (MethodInvocation)invocation;
      
      Destination dest = (Destination)mi.getArguments()[0];
      
      ProducerAdvised del = (ProducerAdvised)invocation.getTargetObject();
      ServerProducerEndpoint prod = (ServerProducerEndpoint)del.getEndpoint();
                  
      if (dest != null)
      {
         //Anonymous producer
         check(mi, dest, CheckType.WRITE, prod.getSessionEndpoint().getConnectionEndpoint());
      }
      
      return invocation.invokeNext();
   }   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private void check(Invocation inv, Destination dest, CheckType checkType, ServerConnectionEndpoint conn)
      throws JMSSecurityException
   {
      JBossDestination jbDest = (JBossDestination)dest;           
      SecurityManager securityManager = conn.getServerPeer().getSecurityManager();      
      SecurityMetadata securityMetadata = securityManager.getSecurityMetadata(jbDest.getName());
      if (securityMetadata == null)
      {
         throw new JMSSecurityException("No security configuration avaliable for " + jbDest.getName());         
      }
      
      //Authenticate
      securityManager.authenticate(conn.getUsername(), conn.getPassword());
      
      //Authorize
      Set principals = checkType == CheckType.READ ? securityMetadata.getReadPrincipals() : 
                       checkType == CheckType.WRITE ? securityMetadata.getWritePrincipals() :
                       securityMetadata.getCreatePrincipals();
                       
      if (!securityManager.authorize(conn.getUsername(), principals))
      {
         String msg = "User: " + conn.getUsername() + 
            " is not authorized to " +
            (checkType == CheckType.READ ? "read from" : 
             checkType == CheckType.WRITE ? "write to" : "create durable sub on") +
            " destination " + jbDest.getName();
             
         throw new JMSSecurityException(msg);                        
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
      public static CheckType READ = new CheckType(0);
      public static CheckType WRITE = new CheckType(1);
      public static CheckType CREATE = new CheckType(2);      
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





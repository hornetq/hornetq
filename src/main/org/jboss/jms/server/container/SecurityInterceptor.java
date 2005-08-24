/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.container;

import java.lang.reflect.Method;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSSecurityException;

import org.jboss.aop.advice.Interceptor;
import org.jboss.aop.joinpoint.Invocation;
import org.jboss.aop.joinpoint.MethodInvocation;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.server.endpoint.ServerConnectionDelegate;
import org.jboss.jms.server.security.SecurityManager;
import org.jboss.jms.server.security.SecurityMetadata;
import org.jboss.logging.Logger;

/**
 * SecurityInterceptor enforces any security constraints on the JMS API
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SecurityInterceptor implements Interceptor
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SecurityInterceptor.class);
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   // Interceptor implementation ------------------------------------
   
   public String getName()
   {
      return "SecurityInterceptor";
   }
   
   public Object invoke(Invocation invocation) throws Throwable
   {
      if (invocation instanceof MethodInvocation)
      {
         MethodInvocation mi = (MethodInvocation)invocation;
         Method m = mi.getMethod();
         String methodName = m.getName();
         JMSAdvisor jmsAdvisor = (JMSAdvisor)mi.getAdvisor();
         
         if ("createConsumerDelegate".equals(methodName))
         {
            
            if (log.isTraceEnabled()) { log.trace("Checking if user has permissions to create consumer"); }
            
            //read permission required on the destination
            Destination dest = (Destination)mi.getArguments()[0];
            
            check(mi, dest, CheckType.READ);
            
            //if creating a durable subscription then need create permission
            
            String subscriptionName = (String)mi.getArguments()[3];
            if (subscriptionName != null)
            {
               //durable
               check(mi, dest, CheckType.CREATE);
            }
            
         } 
         else if ("createProducerDelegate".equals(methodName))
         {
            //write permission required on the destination
            
            //Null represents an anonymous producer - the destination
            //for this is specified at send time
            
            if (log.isTraceEnabled()) { log.trace("Checking if user has permissions to create producer"); }
            
            Destination dest = (Destination)mi.getArguments()[0];
            
            if (dest != null)
            {               
               check(mi, dest, CheckType.WRITE);
            }
         } 
         else if ("createBrowserDelegate".equals(methodName))
         {
            //read permission required on the destination
            
            if (log.isTraceEnabled()) { log.trace("Checking if user has permissions to create browser"); }
            
            Destination dest = (Destination)mi.getArguments()[0];
            
            check(mi, dest, CheckType.READ);
         } 
         else if ("send".equals(methodName))
         {
            //anonymous producer
            
            //if destination is not null then write permissions required
            
            if (log.isTraceEnabled()) { log.trace("Checking if user has permissions to send on anon. producer"); }
            
            Destination dest = (Destination)mi.getArguments()[0];
            
            if (dest != null)
            {
               //Anonymous producer
               check(mi, dest, CheckType.WRITE);
            }
         }         
      }
      
      return invocation.invokeNext();
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private String getConnectionID(Invocation inv)
   {
      return (String)inv.getMetaData().getMetaData(JMSAdvisor.JMS,JMSAdvisor.CONNECTION_ID);
   }
   
   private void check(Invocation inv, Destination dest, CheckType checkType)
      throws JMSSecurityException
   {
      JBossDestination jbDest = (JBossDestination)dest;      
      ServerConnectionDelegate conn = getConn(inv);      
      SecurityManager securityManager = getSecurityManager(inv);      
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
   
   private SecurityManager getSecurityManager(Invocation inv)
   {
      JMSAdvisor jmsAdvisor = (JMSAdvisor)inv.getAdvisor();
      return jmsAdvisor.getServerPeer().getSecurityManager();
   }
   
   private ServerConnectionDelegate getConn(Invocation inv)
   {
      String connectionID = getConnectionID(inv);
      JMSAdvisor jmsAdvisor = (JMSAdvisor)inv.getAdvisor();
      ServerConnectionDelegate conn =
         jmsAdvisor.getServerPeer().getClientManager().getConnectionDelegate(connectionID);
      return conn;
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





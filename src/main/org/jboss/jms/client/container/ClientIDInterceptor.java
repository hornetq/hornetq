/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.Interceptor;
import org.jboss.aop.Invocation;
import org.jboss.aop.MethodInvocation;

/**
 * An interceptor for checking the client id.
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class ClientIDInterceptor
   implements Interceptor
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The client id */
   private String clientID;
   
   /** Can we set the client id */
   private boolean determinedClientID = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Interceptor implementation -----------------------------------

   public String getName()
   {
      return "ClientIDInterceptor";
   }

   public Object invoke(Invocation invocation) throws Throwable
   {
      String methodName = ((MethodInvocation) invocation).method.getName();
      if (methodName.equals("getClientID"))
         return clientID;

      if (methodName.equals("setClientID"))
      {
         setClientID(invocation);
         return null;
      }

      addClientID(invocation);
      try
      {
         return invocation.invokeNext();
      }
      finally
      {
         if (determinedClientID == false)
         {
            clientID = (String) invocation.getResponseAttachment("JMSClientID");
            if (clientID == null)
               throw new IllegalStateException("Unable to determine clientID");
            determinedClientID = true;
         }
      }
   }

   // Protected ------------------------------------------------------

   /**
    * Add the client id or piggy back the request
    * for a client id on this invocation
    * 
    * @param invocation the invocation
    */
   protected void addClientID(Invocation invocation)
      throws Throwable
   {
      if (determinedClientID)
         invocation.getMetaData().addMetaData("JMS", "clientID", clientID);
      else
         invocation.getMetaData().addMetaData("JMS", "clientID", null);
   }

   /**
    * Set the client id
    * 
    * @param invocation the invocation
    */
   protected void setClientID(Invocation invocation)
      throws Throwable
   {
      if (determinedClientID)
         throw new IllegalStateException("Client id is already set");

      MethodInvocation mi = (MethodInvocation) invocation;
      clientID = (String) mi.arguments[0];
      if (clientID == null)
         throw new IllegalArgumentException("Null client id");

      invocation.invokeNext();
      determinedClientID = true;
   }

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}

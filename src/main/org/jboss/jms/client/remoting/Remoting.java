/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.remoting;

import org.jboss.remoting.transport.Connector;
import org.jboss.remoting.InvokerLocator;

/**
 * Remoting static utilities.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Remoting
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // TODO make possible to specify this URI as a parameter
   private static String callbackServerURI = "socket://localhost:2222";
   private static Connector callbackServer;

   /**
    * This mehtod won't be necessary as soon as Remoting supports an UIL2-like transport.
    */
   public static Connector getCallbackServer() throws Exception
   {
      if (callbackServer == null)
      {
         callbackServer = new Connector();
         callbackServer.setInvokerLocator(new InvokerLocator(callbackServerURI).getLocatorURI());
         callbackServer.start();

         // TODO this is unnecessary
         callbackServer.addInvocationHandler("JMS", new ServerInvocationHandlerImpl());
      }
      return callbackServer;
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

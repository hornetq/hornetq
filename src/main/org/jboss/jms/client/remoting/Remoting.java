/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.remoting;

import org.jboss.remoting.transport.Connector;

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

   /**
    * TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)
    */
   public static Connector getCallbackServer() throws Exception
   {
      Connector callbackServer = new Connector();

      // usa an anonymous port
      String locatorURI = "socket://localhost:0";

      callbackServer.setInvokerLocator(locatorURI);
      callbackServer.start();

      // TODO this is unnecessary
      callbackServer.addInvocationHandler("JMS", new ServerInvocationHandlerImpl());

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

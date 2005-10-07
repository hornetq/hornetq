/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.remoting;

import org.jboss.jms.client.container.ConsumerInterceptor;
import org.jboss.logging.Logger;
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
   
   private static final Logger log = Logger.getLogger(Remoting.class);


   // Static --------------------------------------------------------

   /**
    * TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)
    * 
    * Also, if you try and create more than about 5 receivers concurrently in the same JVMs
    * it folds.
    * We need to get the UIL2 style transaport ASAP.
    * This way of doing these is also far too heavy on os resources (e.g. sockets)
    * 
    */
   public static synchronized Connector getCallbackServer() throws Exception
   {
      Connector callbackServer = new Connector();

      // use an anonymous port - 0.0.0.0 gets filled in by remoting as the current host
      String locatorURI = "socket://0.0.0.0:0";
      
      callbackServer.setInvokerLocator(locatorURI);
      
      callbackServer.start();
      
      log.info("Started callback server on: " +  callbackServer.getLocator());

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

/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.tools.jmx;

import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.transport.Connector;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RemotingJMXWrapper implements RemotingJMXWrapperMBean
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InvokerLocator locator;
   private Connector connector;

   // Constructors --------------------------------------------------

   public RemotingJMXWrapper(InvokerLocator locator)
   {
      this.locator = locator;
   }

   // RemotingJMXWrapper implementation -----------------------------

   public void start() throws Exception
   {
      if (connector != null)
      {
         return;
      }

      connector = new Connector();
      connector.setInvokerLocator(locator.getLocatorURI());
      connector.start();

   }

   public void stop() throws Exception
   {
      if (connector == null)
      {
         return;
      }

      connector.stop();
      connector = null;
   }

   public RemotingJMXWrapper getInstance()
   {
      return this;
   }

   public Connector getConnector() throws Exception
   {
      return connector;
   }

   public String getInvokerLocator() throws Exception
   {
      if (connector != null)
      {
         return connector.getInvokerLocator();
      }
      return null;
   }

   public ServerInvocationHandler addInvocationHandler(String s, ServerInvocationHandler h)
      throws Exception
   {
      if (connector != null)
      {
         return connector.addInvocationHandler(s, h);
      }
      return null;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

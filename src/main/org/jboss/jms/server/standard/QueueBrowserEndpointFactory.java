/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.standard;

import org.jboss.jms.server.BrowserEndpoint;
import org.jboss.jms.server.BrowserEndpointFactory;
import org.jboss.jms.server.list.MessageList;

/**
 * A queue delivery endpoint factory
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class QueueBrowserEndpointFactory
   implements BrowserEndpointFactory
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The message list */
   private MessageList list;

   /** The selector */
   private String selector;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueBrowserEndpointFactory(MessageList list, String selector)
   {
      this.list = list;
      this.selector = selector;
   }

   // Public --------------------------------------------------------

   // BrowserEndpointFactory implementation -------------------------

   public BrowserEndpoint getBrowserEndpoint()
   {
      return new QueueBrowserEndpoint(list, selector);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}

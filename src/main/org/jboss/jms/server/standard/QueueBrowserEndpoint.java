/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.standard;

import java.util.List;

import org.jboss.jms.server.BrowserEndpoint;
import org.jboss.jms.server.list.MessageList;

/**
 * A queue browser endpoint
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class QueueBrowserEndpoint
   implements BrowserEndpoint
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The message list */
   private MessageList list;

   /** The selector */
   private String selector;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueBrowserEndpoint(MessageList list, String selector)
   {
      this.list = list;
      this.selector = selector;
   }

   // Public --------------------------------------------------------

   // BrowserEndpoint implementation --------------------------------

   public List browse()
      throws Exception
   {
      return list.browse(selector);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}

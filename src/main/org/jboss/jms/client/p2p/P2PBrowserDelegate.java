/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.p2p;

import java.util.List;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.jboss.messaging.jms.client.BrowserDelegate;

/**
 * The p2p browser
 * 
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class P2PBrowserDelegate
   implements BrowserDelegate
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private P2PSessionDelegate session = null;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public P2PBrowserDelegate(P2PSessionDelegate session, Destination destination, String selector)
      throws JMSException
   {
      this.session = session;
   }

   // Public --------------------------------------------------------

   // BrowserDelegate implementation --------------------------------

	public void close() throws JMSException
	{
	}

	public void closing() throws JMSException
	{
	}

   public List browse() throws JMSException
   {
      return null;
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}

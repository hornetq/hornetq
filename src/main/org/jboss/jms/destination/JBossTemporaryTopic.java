/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.destination;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

import org.jboss.jms.client.ConnectionDelegate;

/**
 * A temporary topic
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossTemporaryTopic
   extends JBossTopic
   implements TemporaryTopic, JBossTemporaryDestination
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The connection */
   private ConnectionDelegate delegate;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Construct a new temporary topic
    * 
    * @param delegate the connection
    * @param name the name
    */
   public JBossTemporaryTopic(ConnectionDelegate delegate, String name)
   {
      super(name);
      this.delegate = delegate;
   }

   // Public --------------------------------------------------------

   // TemporaryTopic implementation ---------------------------------

   public void delete()
      throws JMSException
   {
      delegate.deleteTempDestination(this);
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}

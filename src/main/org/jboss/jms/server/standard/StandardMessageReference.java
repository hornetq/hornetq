/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.standard;

import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.MessageReference;

/**
 * A message reference
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class StandardMessageReference
   implements MessageReference
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The message */
   private JBossMessage message;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public StandardMessageReference(JBossMessage message)
   {
      this.message = message;
   }

   // Public --------------------------------------------------------

   // MessageReference implementation -------------------------------

   public JBossMessage getMessage()
      throws Exception
   {
      return message;
   }

   public int getPriority()
      throws Exception
   {
      return message.getJMSPriority();
   }
   
   public String getMessageID()
      throws Exception
   {
      return message.getJMSMessageID();
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}

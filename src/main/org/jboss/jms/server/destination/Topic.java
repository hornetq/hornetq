/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.destination;

/**
 * A deployable JBoss Messaging topic.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Topic extends DeployableDestinationSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public Topic()
   {
      super(null);
   }

   public Topic(String jndiName)
   {
      super(jndiName);
   }

   // JMX managed attributes ----------------------------------------

   // JMX managed operations ----------------------------------------

   // TODO implement these:

//   int getAllMessageCount();
//
//   int getDurableMessageCount();
//
//   int getNonDurableMessageCount();
//
//   int getAllSubscriptionsCount();
//
//   int getDurableSubscriptionsCount();
//
//   int getNonDurableSubscriptionsCount();
//
//   java.util.List listAllSubscriptions();
//
//   java.util.List listDurableSubscriptions();
//
//   java.util.List listNonDurableSubscriptions();
//
//   java.util.List listMessages(java.lang.String id) throws java.lang.Exception;
//
//   java.util.List listMessages(java.lang.String id, java.lang.String selector) throws java.lang.Exception;
//
//   List listNonDurableMessages(String id, String sub) throws Exception;
//
//   List listNonDurableMessages(String id, String sub, String selector) throws Exception;
//
//   List listDurableMessages(String id, String name) throws Exception;
//
//   List listDurableMessages(String id, String name, String selector) throws Exception;

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected boolean isQueue()
   {
      return false;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.container;

import org.jboss.aop.AspectManager;
import org.jboss.aop.ClassAdvisor;
import org.jboss.jms.server.ServerPeer;

/**
 * A ClassAdvisor that keeps server peer-specific state.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSAdvisor extends ClassAdvisor
{
   // Constants -----------------------------------------------------

   // TODO relocate these constants to a different class, since they're used both on client and server-side, and not only in an AOP context
   public static final String JMS = "JMS";
   
   //These are used to locate the server side delegate instance
   //When we refactor to use JBoss AOP's remote proxy these will become unnecessary
   public static final String CONNECTION_ID = "CONNECTION_ID";
   public static final String SESSION_ID = "SESSION_ID";
   public static final String PRODUCER_ID = "PRODUCER_ID";
   public static final String CONSUMER_ID = "CONSUMER_ID";
   public static final String BROWSER_ID = "BROWSER_ID";
   
   public static final String REMOTING_SESSION_ID = "REMOTING_SESSION_ID";
   public static final String CALLBACK_HANDLER = "CALLBACK_HANDLER";
   public static final String CONNECTION_FACTORY_ID = "CONNECTION_FACTORY_ID";
   public static final String EVICTED_MESSAGE_IDS = "EVICTED_MESSAGE_IDS";


   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;


   // Constructors --------------------------------------------------

   public JMSAdvisor(String classname, AspectManager manager, ServerPeer serverPeer)
   {
      super(classname, manager);
      this.serverPeer = serverPeer;
   }

   // Public --------------------------------------------------------

   
   public ServerPeer getServerPeer()
   {
      return serverPeer;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

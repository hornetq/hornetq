/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.container;

import org.jboss.aop.ClassAdvisor;
import org.jboss.aop.AspectManager;
import org.jboss.jms.server.ServerPeer;

/**
 * A ClassAdvisor that keeps server peer-specific state.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JMSAdvisor extends ClassAdvisor
{
   // Constants -----------------------------------------------------

   public static final String JMS = "JMS";
   public static final String CLIENT_ID = "CLIENT_ID";
   public static final String SESSION_ID = "SESSION_ID";
   public static final String PRODUCER_ID = "PRODUCER_ID";
   public static final String CONSUMER_ID = "CONSUMER_ID";
   public static final String REMOTING_SESSION_ID = "REMOTING_SESSION_ID";
   public static final String CALLBACK_HANDLER = "CALLBACK_HANDLER";
   public static final String DELIVERY_MODE = "DELIVERY_MODE";
   public static final String XID = "XID";
   public static final String EXCEPTION_LISTENER = "E_LISTENER";

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

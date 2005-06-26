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

   // TODO relocate these constants to a different class, since they're used both on client and server-side, and not only in an AOP context
   public static final String JMS = "JMS";
   public static final String CONNECTION_ID = "CONNECTION_ID";
   public static final String SESSION_ID = "SESSION_ID";
   public static final String PRODUCER_ID = "PRODUCER_ID";
   public static final String CONSUMER_ID = "CONSUMER_ID";
   public static final String BROWSER_ID = "BROWSER_ID";
   public static final String REMOTING_SESSION_ID = "REMOTING_SESSION_ID";
   public static final String CALLBACK_HANDLER = "CALLBACK_HANDLER";
   public static final String XID = "XID";
   public static final String EXCEPTION_LISTENER = "EXCEPTION_LISTENER";
	public static final String ACKNOWLEDGMENT_MODE = "ACKNOWLEDGMENT_MODE";
	public static final String UNACKED = "UNACKED";
	public static final String RESOURCE_MANAGER = "RESOURCE_MGR";
   public static final String CLIENT_ID = "CLIENT_ID";
   public static final String CONNECTION_FACTORY_ID = "CONNECTION_FACTORY_ID";

   public static final String DELIVERY_MODE = "DELIVERY_MODE";
   public static final String IS_MESSAGE_ID_DISABLED = "IS_MESSAGE_ID_DISABLED";
   public static final String IS_MESSAGE_TIMESTAMP_DISABLED = "IS_MESSAGE_TIMESTAMP_DISABLED";
   public static final String SELECTOR = "SELECTOR";
   public static final String CONNECTION_META_DATA = "CONNECTION_META_DATA ";
   public static final String PRIORITY = "PRIORITY";
   public static final String TIME_TO_LIVE = "TIME_TO_LIVE";
   public static final String TRANSACTED = "TRANSACTED";
   public static final String DESTINATION = "DESTINATION";
   public static final String NO_LOCAL = "NO_LOCAL";


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

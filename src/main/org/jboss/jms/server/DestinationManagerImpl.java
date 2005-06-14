/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.logging.Logger;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.jms.util.JNDIUtil;
import org.jboss.messaging.core.local.AbstractDestination;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.jms.Destination;
import javax.jms.JMSException;
import java.util.Map;
import java.util.HashMap;

/**
 * Manages destinations. Manages JNDI mapping and delegates core destination management to a
 * CoreDestinationManager.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DestinationManagerImpl implements DestinationManagerImplMBean
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(DestinationManagerImpl.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;
   protected Context initialContext;
   protected CoreDestinationManager coreDestinationManager;

   // < name - JNDI name>
   protected Map nameToJNDI;


   // Constructors --------------------------------------------------

   public DestinationManagerImpl(ServerPeer serverPeer) throws Exception
   {
      this.serverPeer = serverPeer;
      initialContext = new InitialContext(serverPeer.getJNDIEnvironment());
      coreDestinationManager = new CoreDestinationManager(this);
      nameToJNDI = new HashMap();
   }

   // DestinationManager implementation -----------------------------


   public void createQueue(String name, String jndiName) throws Exception
   {
      createDestination(true, name, jndiName);
   }

   public void destroyQueue(String name) throws Exception
   {
      removeDestination(name);
   }

   public void createTopic(String name, String jndiName) throws Exception
   {
      createDestination(false, name, jndiName);
   }

   public void destroyTopic(String name) throws Exception
   {
      removeDestination(name);
   }

   public void createQueue(String name) throws Exception
   {
      createDestination(true, name, null);
   }

   public void createTopic(String name) throws Exception
   {
      createDestination(false, name, null);
   }


   // Public --------------------------------------------------------

   ServerPeer getServerPeer()
   {
      return serverPeer;
   }

   public void addTemporaryDestination(Destination jmsDestination) throws JMSException
   {
      coreDestinationManager.addCoreDestination(jmsDestination);
   }

   public void removeTemporaryDestination(Destination jmsDestination)
   {
      coreDestinationManager.removeCoreDestination(((JBossDestination)jmsDestination).getName());
   }

   public AbstractDestination getCoreDestination(Destination jmsDestination) throws JMSException
   {
      return getCoreDestination(((JBossDestination)jmsDestination).getName());
   }

   public AbstractDestination getCoreDestination(String name) throws JMSException
   {
      return coreDestinationManager.getCoreDestination(name);
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void createDestination(boolean isQueue, String name, String jndiName) throws Exception
   {
      String parentContext;
      String jndiNameInContext;

      if (jndiName == null)
      {
         parentContext = (isQueue ? DEFAULT_QUEUE_CONTEXT : DEFAULT_TOPIC_CONTEXT);
         jndiNameInContext = name;
         jndiName = parentContext + "/" + jndiNameInContext;
      }
      else
      {
         // TODO more solid parsing + test cases
         int sepIndex = jndiName.lastIndexOf('/');
         if (sepIndex == -1)
         {
            parentContext = "";
         }
         else
         {
            parentContext = jndiName.substring(0, sepIndex);
         }
         jndiNameInContext = jndiName.substring(sepIndex + 1);
      }

      try
      {
         initialContext.lookup(jndiName);
         throw new JBossJMSException("JNDI binding " + jndiName + " already exists");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      Destination jmsDestination = isQueue ?
                                   (Destination) new JBossQueue(name) :
                                   (Destination) new JBossTopic(name);

      coreDestinationManager.addCoreDestination(jmsDestination);

      try
      {
         Context c = JNDIUtil.createContext(initialContext, parentContext);
         c.bind(jndiNameInContext, jmsDestination);
         nameToJNDI.put(name, jndiName);
      }
      catch(Exception e)
      {
         coreDestinationManager.removeCoreDestination(name);
         throw e;
      }

      log.info((isQueue ? "Queue" : "Topic") + " " + name +
               " created and bound in JNDI as " + jndiName );
   }


   private void removeDestination(String name) throws Exception
   {

      coreDestinationManager.removeCoreDestination(name);
      String jndiName = (String)nameToJNDI.get(name);
      if (jndiName == null)
      {
         return;
      }
      initialContext.unbind(jndiName);
   }

   // Inner classes -------------------------------------------------
}

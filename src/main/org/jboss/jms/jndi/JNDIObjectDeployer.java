/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
   * by the @authors tag. See the copyright.txt in the distribution for a
   * full listing of individual contributors.
   *
   * This is free software; you can redistribute it and/or modify it
   * under the terms of the GNU Lesser General Public License as
   * published by the Free Software Foundation; either version 2.1 of
   * the License, or (at your option) any later version.
   *
   * This software is distributed in the hope that it will be useful,
   * but WITHOUT ANY WARRANTY; without even the implied warranty of
   * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   * Lesser General Public License for more details.
   *
   * You should have received a copy of the GNU Lesser General Public
   * License along with this software; if not, write to the Free
   * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
   * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
   */
package org.jboss.jms.jndi;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.impl.ClientConnectionFactoryImpl;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.server.connectionfactory.ConnectionFactory;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryEndpoint;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.remoting.ServerLocator;
import org.jboss.messaging.deployers.Deployer;
import org.jboss.messaging.deployers.DeploymentManager;
import org.jboss.messaging.util.JNDIUtil;
import org.jboss.messaging.util.Version;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.jms.InvalidDestinationException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

/**
 * A Deployer used to create and add to JNDI queues, topics and connection factories. Typically this would only be used
 * in an app server env.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JNDIObjectDeployer extends Deployer
{
   Logger log = Logger.getLogger(JNDIObjectDeployer.class);

   /**
    * the initial context to bind to
    */
   InitialContext initialContext;

   /**
    * The messagingserver used for creating the objects
    */
   MessagingServer messagingServer;

   private static final String CLIENTID_ELEMENT = "client-id";
   private static final String PREFETECH_SIZE_ELEMENT = "prefetch-size";
   private static final String DEF_TEMP_Q_FULL_SIZE = "default-temp-queue-full-size";
   private static final String DEF_TEMP_Q_PAGE_SIZE_SIZE = "default-temp-queue-page-size";
   private static final String DEF_TEMP_Q_DOWN_CACHE_SIZE = "default-temp-queue-down-cache-size";
   private static final String DUPS_OK_BATCH_SIZE = "dups-ok-batch-size";
   private static final String SUPPORTS_FAILOVER = "supports-failover";
   private static final String SUPPORTS_LOAD_BALANCING = "supports-load-balancing";
   private static final String LOAD_BALANCING_FACTORY = "load-balancing-factory";
   private static final String STRICT_TCK = "strict-tck";
   private static final String DISABLE_REMOTING_CHECKS = "disable-remoting-checks";
   private static final String ENTRY_NODE_NAME = "entry";
   private static final String CONNECTION_FACTORY_NODE_NAME = "connection-factory";
   private static final String QUEUE_NODE_NAME = "queue";
   private static final String TOPIC_NODE_NAME = "topic";

   public void setMessagingServer(MessagingServer messagingServer)
   {
      this.messagingServer = messagingServer;
   }

   /**
    * lifecycle method
    */
   public void start()
   {
      try
      {
         initialContext = new InitialContext();
      }
      catch (NamingException e)
      {
         log.error("Unable to create Initial Context", e);
      }
      try
      {
         DeploymentManager.getInstance().registerDeployable(this);
      }
      catch (Exception e)
      {
         log.error(new StringBuilder("Unable to get Deployment Manager: ").append(e));
      }
   }

   /**
    * lifecycle method
    */
   public void stop()
   {

   }

   /**
    * the names of the elements to deploy
    * @return the names of the elements todeploy
    */
   public String[] getElementTagName()
   {
      return new String[]{QUEUE_NODE_NAME, TOPIC_NODE_NAME, CONNECTION_FACTORY_NODE_NAME};
   }

   /**
    * deploy an element
    * @param node the element to deploy
    * @throws Exception .
    */
   public void deploy(Node node) throws Exception
   {
      Object objectToBind = getObjectToBind(node);
      NodeList children = node.getChildNodes();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);

         if (ENTRY_NODE_NAME.equalsIgnoreCase(children.item(i).getNodeName()))
         {

            String jndiName = child.getAttributes().getNamedItem("name").getNodeValue();
            String parentContext;
            String jndiNameInContext;
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
            try
            {
               initialContext.lookup(jndiName);
               throw new InvalidDestinationException("Destination " + jndiName + " already exists");
            }
            catch (NameNotFoundException e)
            {
               // OK
            }

            Context c = JNDIUtil.createContext(initialContext, parentContext);

            c.rebind(jndiNameInContext, objectToBind);
         }
      }
   }

   /**
    * creates the object to bind, this will either be a JBossConnectionFActory, JBossQueue or JBossTopic
    * @param node the config
    * @return the object to bind
    * @throws Exception .
    */
   private Object getObjectToBind(Node node) throws Exception
   {
      if (node.getNodeName().equals(CONNECTION_FACTORY_NODE_NAME))
      {
         ConnectionFactory connectionFactory = createConnectionFactory(node);
         ServerLocator serverLocator = messagingServer.getMinaService().getLocator();

         log.info("Server locator is " + serverLocator);
         log.info(this + " started");
         // See http://www.jboss.com/index.html?module=bb&op=viewtopic&p=4076040#4076040
         final String id = connectionFactory.getName();

         Version version = messagingServer.getVersion();

         ServerConnectionFactoryEndpoint endpoint =
                 new ServerConnectionFactoryEndpoint(connectionFactory.getName(), id, messagingServer, connectionFactory.getClientID(),
                         connectionFactory.getPrefetchSize(),
                         connectionFactory.getDefaultTempQueueFullSize(),
                         connectionFactory.getDefaultTempQueuePageSize(),
                         connectionFactory.getDefaultTempQueueDownCacheSize(),
                         connectionFactory.getDupsOKBatchSize());

         //The server peer strict setting overrides the connection factory
         boolean useStrict = messagingServer.getConfiguration().isStrictTck() || connectionFactory.isStrictTck();

         ClientConnectionFactoryImpl delegate =
                 new ClientConnectionFactoryImpl(id, messagingServer.getConfiguration().getMessagingServerID(),
                         serverLocator.getURI(), version, false, useStrict);

         log.debug(this + " created local delegate " + delegate);

         // Registering with the dispatcher should always be the last thing otherwise a client could
         // use a partially initialised object

         messagingServer.getMinaService().getDispatcher().register(endpoint.newHandler());
         return new JBossConnectionFactory(delegate);
      }
      else if (node.getNodeName().equals(QUEUE_NODE_NAME))
      {
         String queueName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         messagingServer.createQueue(queueName);
         return new JBossQueue(queueName);

      }
      else if (node.getNodeName().equals(TOPIC_NODE_NAME))
      {
         String topicName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         messagingServer.createTopic(topicName);
         return new JBossTopic(topicName);

      }
      return null;
   }

   /**
    * undeploys an element
    * @param node the element to undeploy
    * @throws Exception .
    */
   public void undeploy(Node node) throws Exception
   {
      System.out.println("JNDIObjectDeployer.undeploy");
   }

   /**
    * The name of the configuration file name to look for for deployment
    *
    * @return The name of the config file
    */
   public String getConfigFileName()
   {
      return "jbm-jndi.xml";
   }

   /**
    * creates a connection factory
    * @param node the config
    * @return a ConnectionFactory
    * @throws Exception .
    */
   private ConnectionFactory createConnectionFactory(Node node) throws Exception
   {
      String clientId = null;
      String name = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
      ConnectionFactory connectionFactory = new ConnectionFactory(clientId);
      connectionFactory.setName(name);

      NodeList attributes = node.getChildNodes();
      for (int j = 0; j < attributes.getLength(); j++)
      {
         if (CLIENTID_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            clientId = attributes.item(j).getTextContent();
            connectionFactory.setClientID(clientId);
         }

         if (PREFETECH_SIZE_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            connectionFactory.setPrefetchSize(Integer.parseInt(attributes.item(j).getTextContent().trim()));
         }
         if (DEF_TEMP_Q_FULL_SIZE.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            connectionFactory.setDefaultTempQueueFullSize(Integer.parseInt(attributes.item(j).getTextContent().trim()));
         }
         if (DEF_TEMP_Q_PAGE_SIZE_SIZE.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            connectionFactory.setDefaultTempQueuePageSize(Integer.parseInt(attributes.item(j).getTextContent().trim()));
         }
         if (DEF_TEMP_Q_DOWN_CACHE_SIZE.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            connectionFactory.setDefaultTempQueueDownCacheSize(Integer.parseInt(attributes.item(j).getTextContent().trim()));
         }
         if (DUPS_OK_BATCH_SIZE.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            connectionFactory.setDupsOKBatchSize(Integer.parseInt(attributes.item(j).getTextContent().trim()));
         }
         if (SUPPORTS_FAILOVER.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            connectionFactory.setSupportsFailover(Boolean.parseBoolean(attributes.item(j).getTextContent().trim()));
         }
         if (SUPPORTS_LOAD_BALANCING.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            connectionFactory.setSupportsLoadBalancing(Boolean.parseBoolean(attributes.item(j).getTextContent().trim()));
         }
         if (LOAD_BALANCING_FACTORY.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            connectionFactory.setLoadBalancingFactory(attributes.item(j).getTextContent().trim());
         }
         if (STRICT_TCK.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            connectionFactory.setStrictTck(Boolean.parseBoolean(attributes.item(j).getTextContent().trim()));
         }
         if (DISABLE_REMOTING_CHECKS.equalsIgnoreCase(attributes.item(j).getNodeName()))
         {
            connectionFactory.setDisableRemotingChecks(Boolean.parseBoolean(attributes.item(j).getTextContent().trim()));
         }

      }
      return connectionFactory;
   }

}

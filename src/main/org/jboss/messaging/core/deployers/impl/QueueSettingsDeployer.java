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
package org.jboss.messaging.core.deployers.impl;

import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A deployer for creating a set of queue settings and adding them to a repository
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class QueueSettingsDeployer extends XmlDeployer
{   
   private static final String CLUSTERED_NODE_NAME = "clustered";
   
   private static final String DLQ_NODE_NAME = "dlq";
   
   private static final String EXPIRY_QUEUE_NODE_NAME = "expiry-queue";
   
   private static final String REDELIVERY_DELAY_NODE_NAME = "redelivery-delay";
   
   private static final String MAX_SIZE_BYTES_NODE_NAME = "max-size-bytes";
   
   private static final String DISTRIBUTION_POLICY_CLASS_NODE_NAME = "distribution-policy-class";
   
   private static final String MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME = "message-counter-history-day-limit";

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;
   
   public QueueSettingsDeployer(final HierarchicalRepository<QueueSettings> queueSettingsRepository)
   {
   	this.queueSettingsRepository = queueSettingsRepository;
   }
   
   /**
    * the names of the elements to deploy
    * @return the names of the elements todeploy
    */
   public String[] getElementTagName()
   {
      return new String[]{"queue-settings"};
   }

   /**
    * deploy an element
    * @param node the element to deploy
    * @throws Exception .
    */
   public void deploy(Node node) throws Exception
   {
      String match = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
      
      NodeList children = node.getChildNodes();
      
      QueueSettings queueSettings = new QueueSettings();
      
      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);
         
         if (CLUSTERED_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            queueSettings.setClustered(Boolean.valueOf(child.getTextContent()));
         }
         else if (DLQ_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            SimpleString queueName = new SimpleString(child.getTextContent());
            queueSettings.setDLQ(queueName);
         }
         else if (EXPIRY_QUEUE_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
         	SimpleString queueName = new SimpleString(child.getTextContent());
            queueSettings.setExpiryQueue(queueName);
         }
         else if (REDELIVERY_DELAY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            queueSettings.setRedeliveryDelay(Long.valueOf(child.getTextContent()));
         }
         else if (MAX_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            queueSettings.setMaxSizeBytes(Integer.valueOf(child.getTextContent()));   
         }
         else if (DISTRIBUTION_POLICY_CLASS_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            queueSettings.setDistributionPolicyClass(child.getTextContent());
         }
         else if (MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            queueSettings.setMessageCounterHistoryDayLimit(Integer.valueOf(child.getTextContent()));
         }
      }
      
      queueSettingsRepository.addMatch(match, queueSettings);
   }

   /**
    * The name of the configuration file name to look for for deployment
    *
    * @return The name of the config file
    */
   public String getConfigFileName()
   {
      return "queues.xml";
   }

   public int getWeight()
   {
      return 1;
   }

   /**
    * undeploys an element
    * @param node the element to undeploy
    * @throws Exception .
    */
   public void undeploy(Node node) throws Exception
   {
      String match = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
      
      queueSettingsRepository.removeMatch(match);
   }

   /**
    * the key attribute for theelement, usually 'name' but can be overridden
    * @return the key attribute
    */
   public String getKeyAttribute()
   {
      return "match";
   }

}

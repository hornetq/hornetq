/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.utils.SimpleString;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A deployer for creating a set of queue settings and adding them to a repository
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class AddressSettingsDeployer extends XmlDeployer
{
   private static final String DEAD_LETTER_ADDRESS_NODE_NAME = "dead-letter-address";

   private static final String EXPIRY_ADDRESS_NODE_NAME = "expiry-address";

   private static final String REDELIVERY_DELAY_NODE_NAME = "redelivery-delay";

   private static final String MAX_SIZE_BYTES_NODE_NAME = "max-size-bytes";

   private static final String DROP_MESSAGES_WHEN_FULL_NODE_NAME = "drop-messages-when-full";

   private static final String PAGE_SIZE_BYTES_NODE_NAME = "page-size-bytes";

   private static final String DISTRIBUTION_POLICY_CLASS_NODE_NAME = "distribution-policy-class";

   private static final String MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME = "message-counter-history-day-limit";

   private static final String SOLO_MESSAGE_NODE_NAME = "solo-queue";

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   public AddressSettingsDeployer(final DeploymentManager deploymentManager,
                                  final HierarchicalRepository<AddressSettings> addressSettingsRepository)
   {
      super(deploymentManager);
      this.addressSettingsRepository = addressSettingsRepository;
   }

   /**
    * the names of the elements to deploy
    * @return the names of the elements todeploy
    */
   public String[] getElementTagName()
   {
      return new String[] { "address-settings" };
   }

   @Override
   public void validate(Node rootNode) throws Exception
   {
      if ("deployment".equals(rootNode.getNodeName()))
      {
         org.jboss.messaging.utils.XMLUtil.validate(rootNode, "jbm-configuration.xsd");
      }
      else
      {
         org.jboss.messaging.utils.XMLUtil.validate(rootNode, "jbm-queues.xsd");
      }
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

      AddressSettings addressSettings = new AddressSettings();

      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);

         if (DEAD_LETTER_ADDRESS_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            SimpleString queueName = new SimpleString(child.getTextContent());
            addressSettings.setDeadLetterAddress(queueName);
         }
         else if (EXPIRY_ADDRESS_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            SimpleString queueName = new SimpleString(child.getTextContent());
            addressSettings.setExpiryAddress(queueName);
         }
         else if (REDELIVERY_DELAY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setRedeliveryDelay(Long.valueOf(child.getTextContent()));
         }
         else if (MAX_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setMaxSizeBytes(Integer.valueOf(child.getTextContent()));
         }
         else if (PAGE_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setPageSizeBytes(Integer.valueOf(child.getTextContent()));
         }
         else if (DISTRIBUTION_POLICY_CLASS_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setDistributionPolicyClass(child.getTextContent());
         }
         else if (MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setMessageCounterHistoryDayLimit(Integer.valueOf(child.getTextContent()));
         }
         else if (DROP_MESSAGES_WHEN_FULL_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setDropMessagesWhenFull(Boolean.valueOf(child.getTextContent().trim()));
         }
         else if (SOLO_MESSAGE_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setSoloQueue(Boolean.valueOf(child.getTextContent().trim()));
         }
      }

      addressSettingsRepository.addMatch(match, addressSettings);
   }

   public String[] getConfigFileNames()
   {
      return new String[] { "jbm-configuration", "jbm-queues.xml" };
   }

   /**
    * undeploys an element
    * @param node the element to undeploy
    * @throws Exception .
    */
   public void undeploy(Node node) throws Exception
   {
      String match = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();

      addressSettingsRepository.removeMatch(match);
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

/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.deployers.impl;

import org.hornetq.SimpleString;
import org.hornetq.core.config.impl.Validators;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * A deployer for creating a set of queue settings and adding them to a repository
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class AddressSettingsDeployer extends XmlDeployer
{
   private static final Logger log = Logger.getLogger(AddressSettingsDeployer.class);

   private static final String DEAD_LETTER_ADDRESS_NODE_NAME = "dead-letter-address";

   private static final String EXPIRY_ADDRESS_NODE_NAME = "expiry-address";

   private static final String REDELIVERY_DELAY_NODE_NAME = "redelivery-delay";

   private static final String MAX_DELIVERY_ATTEMPTS = "max-delivery-attempts";

   private static final String MAX_SIZE_BYTES_NODE_NAME = "max-size-bytes";

   private static final String ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME = "address-full-policy";

   private static final String PAGE_SIZE_BYTES_NODE_NAME = "page-size-bytes";

   private static final String MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME = "message-counter-history-day-limit";

   private static final String LVQ_NODE_NAME = "last-value-queue";

   private static final String REDISTRIBUTION_DELAY_NODE_NAME = "redistribution-delay";

   private static final String SEND_TO_DLA_ON_NO_ROUTE = "send-to-dla-on-no-route";

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
   @Override
   public String[] getElementTagName()
   {
      return new String[] { "address-setting" };
   }

   @Override
   public void validate(final Node rootNode) throws Exception
   {
      org.hornetq.utils.XMLUtil.validate(rootNode, "schema/hornetq-configuration.xsd");
   }

   /**
    * deploy an element
    * @param node the element to deploy
    * @throws Exception .
    */
   @Override
   public void deploy(final Node node) throws Exception
   {
      String match = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();

      NodeList children = node.getChildNodes();

      AddressSettings addressSettings = new AddressSettings();

      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);

         if (AddressSettingsDeployer.DEAD_LETTER_ADDRESS_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            SimpleString queueName = new SimpleString(child.getTextContent());
            addressSettings.setDeadLetterAddress(queueName);
         }
         else if (AddressSettingsDeployer.EXPIRY_ADDRESS_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            SimpleString queueName = new SimpleString(child.getTextContent());
            addressSettings.setExpiryAddress(queueName);
         }
         else if (AddressSettingsDeployer.REDELIVERY_DELAY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setRedeliveryDelay(Long.valueOf(child.getTextContent()));
         }
         else if (AddressSettingsDeployer.MAX_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setMaxSizeBytes(Integer.valueOf(child.getTextContent()));
         }
         else if (AddressSettingsDeployer.PAGE_SIZE_BYTES_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setPageSizeBytes(Integer.valueOf(child.getTextContent()));
         }
         else if (AddressSettingsDeployer.MESSAGE_COUNTER_HISTORY_DAY_LIMIT_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setMessageCounterHistoryDayLimit(Integer.valueOf(child.getTextContent()));
         }
         else if (AddressSettingsDeployer.ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            String value = child.getTextContent().trim();
            Validators.ADDRESS_FULL_MESSAGE_POLICY_TYPE.validate(AddressSettingsDeployer.ADDRESS_FULL_MESSAGE_POLICY_NODE_NAME,
                                                                 value);
            AddressFullMessagePolicy policy = null;
            if (value.equals(AddressFullMessagePolicy.BLOCK.toString()))
            {
               policy = AddressFullMessagePolicy.BLOCK;
            }
            else if (value.equals(AddressFullMessagePolicy.DROP.toString()))
            {
               policy = AddressFullMessagePolicy.DROP;
            }
            else if (value.equals(AddressFullMessagePolicy.PAGE.toString()))
            {
               policy = AddressFullMessagePolicy.PAGE;
            }
            addressSettings.setAddressFullMessagePolicy(policy);
         }
         else if (AddressSettingsDeployer.LVQ_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setLastValueQueue(Boolean.valueOf(child.getTextContent().trim()));
         }
         else if (AddressSettingsDeployer.MAX_DELIVERY_ATTEMPTS.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setMaxDeliveryAttempts(Integer.valueOf(child.getTextContent().trim()));
         }
         else if (AddressSettingsDeployer.REDISTRIBUTION_DELAY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setRedistributionDelay(Long.valueOf(child.getTextContent().trim()));
         }
         else if (AddressSettingsDeployer.SEND_TO_DLA_ON_NO_ROUTE.equalsIgnoreCase(child.getNodeName()))
         {
            addressSettings.setSendToDLAOnNoRoute(Boolean.valueOf(child.getTextContent().trim()));
         }
      }

      addressSettingsRepository.addMatch(match, addressSettings);
   }

   @Override
   public String[] getDefaultConfigFileNames()
   {
      return new String[] { "hornetq-configuration.xml", "hornetq-queues.xml" };
   }

   /**
    * undeploys an element
    * @param node the element to undeploy
    * @throws Exception .
    */
   @Override
   public void undeploy(final Node node) throws Exception
   {
      String match = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();

      addressSettingsRepository.removeMatch(match);
   }

   /**
    * the key attribute for theelement, usually 'name' but can be overridden
    * @return the key attribute
    */
   @Override
   public String getKeyAttribute()
   {
      return "match";
   }

}

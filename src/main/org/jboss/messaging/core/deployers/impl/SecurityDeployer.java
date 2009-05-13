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
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Deploys the security settings into a security repository and adds them to the security store.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class SecurityDeployer extends XmlDeployer
{
   private static final Logger log = Logger.getLogger(SecurityDeployer.class);  
   
   private static final String PERMISSION_ELEMENT_NAME = "permission";
   private static final String TYPE_ATTR_NAME = "type";
   private static final String ROLES_ATTR_NAME = "roles";
   private static final String QUEUES_XML = "jbm-queues.xml";
   private static final String MATCH = "match";
   private static final String SECURITY_ELEMENT_NAME = "security-setting";

   public static final String SEND_NAME = "send";
   public static final String CONSUME_NAME = "consume";
   public static final String CREATEDURABLEQUEUE_NAME = "createDurableQueue";
   public static final String DELETEDURABLEQUEUE_NAME = "deleteDurableQueue";
   public static final String CREATETEMPQUEUE_NAME = "createTempQueue";
   public static final String DELETETEMPQUEUE_NAME = "deleteTempQueue";
   public static final String MANAGE_NAME = "manage";

   /**
    * The repository to add to
    */
   private final HierarchicalRepository<Set<Role>> securityRepository;

   public SecurityDeployer(final DeploymentManager deploymentManager, final HierarchicalRepository<Set<Role>> securityRepository)
   {
      super(deploymentManager);
      
      this.securityRepository = securityRepository;      
   }

   /**
    * the names of the elements to deploy
    *
    * @return the names of the elements todeploy
    */
   public String[] getElementTagName()
   {
      return new String[]{SECURITY_ELEMENT_NAME};
   }

   @Override
   public void validate(Node rootNode) throws Exception
   {
      org.jboss.messaging.utils.XMLUtil.validate(rootNode, "schema/jbm-configuration.xsd");
   }
   
   /**
    * the key attribute for theelement, usually 'name' but can be overridden
    *
    * @return the key attribute
    */
   public String getKeyAttribute()
   {
      return MATCH;
   }

   /**
    * deploy an element
    *
    * @param node the element to deploy
    * @throws Exception .
    */
   public void deploy(final Node node) throws Exception
   {
      HashSet<Role> securityRoles = new HashSet<Role>();
      ArrayList<String> send = new ArrayList<String>();
      ArrayList<String> consume = new ArrayList<String>();
      ArrayList<String> createDurableQueue = new ArrayList<String>();
      ArrayList<String> deleteDurableQueue = new ArrayList<String>();
      ArrayList<String> createTempQueue = new ArrayList<String>();
      ArrayList<String> deleteTempQueue = new ArrayList<String>();
      ArrayList<String> manageRoles = new ArrayList<String>();
      ArrayList<String> allRoles = new ArrayList<String>();
      String match = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
      NodeList children = node.getChildNodes();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node child = children.item(i);

         if (PERMISSION_ELEMENT_NAME.equalsIgnoreCase(child.getNodeName()))
         {
            String type = child.getAttributes().getNamedItem(TYPE_ATTR_NAME).getNodeValue();
            String roleString = child.getAttributes().getNamedItem(ROLES_ATTR_NAME).getNodeValue();
            String[] roles = roleString.split(",");
            for (String role : roles)
            {
               if (SEND_NAME.equals(type))
               {
                  send.add(role.trim());
               }
               else if (CONSUME_NAME.equals(type))
               {
                  consume.add(role.trim());
               }
               else if (CREATEDURABLEQUEUE_NAME.equals(type))
               {
                  createDurableQueue.add(role);
               }
               else if (DELETEDURABLEQUEUE_NAME.equals(type))
               {
                  deleteDurableQueue.add(role);
               }
               else if (CREATETEMPQUEUE_NAME.equals(type))
               {
                  createTempQueue.add(role);
               }
               else if (DELETETEMPQUEUE_NAME.equals(type))
               {
                  deleteTempQueue.add(role);
               }
               else if (MANAGE_NAME.equals(type))
               {
                  manageRoles.add(role);
               }
               if (!allRoles.contains(role.trim()))
               {
                  allRoles.add(role.trim());
               }
            }
         }

      }
      for (String role : allRoles)
      {
         securityRoles.add(new Role(role,
                                    send.contains(role),
                                    consume.contains(role),
                                    createDurableQueue.contains(role), 
                                    deleteDurableQueue.contains(role),
                                    createTempQueue.contains(role),
                                    deleteTempQueue.contains(role),
                                    manageRoles.contains(role)));
      }
      securityRepository.addMatch(match, securityRoles);
   }

   /**
    * undeploys an element
    *
    * @param node the element to undeploy
    * @throws Exception .
    */
   public void undeploy(final Node node) throws Exception
   {
      String match = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
      securityRepository.removeMatch(match);
   }

   /**
    * The name of the configuration file name to look for for deployment
    *
    * @return The name of the config file
    */
   public String[] getDefaultConfigFileNames()
   {
      return new String[] {"jbm-configuration.xml", QUEUES_XML};
   }
}

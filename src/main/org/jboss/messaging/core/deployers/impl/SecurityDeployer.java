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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Deploys the security settings into a security repository and adds them to the security store.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class SecurityDeployer extends XmlDeployer
{

   private static final String PERMISSION_ELEMENT_NAME = "permission";
   private static final String TYPE_ATTR_NAME = "type";
   private static final String ROLES_ATTR_NAME = "roles";
   private static final String QUEUES_XML = "jbm-queues.xml";
   private static final String MATCH = "match";
   private static final String SECURITY_ELEMENT_NAME = "security";

   public static final String WRITE_NAME = "write";
   public static final String READ_NAME = "read";
   public static final String CREATE_NAME = "create";

   /**
    * The repository to add to
    */
   private HierarchicalRepository<Set<Role>> securityRepository;

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
      XMLUtil.validate(rootNode, "jbm-queues.xsd");
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
      ArrayList<String> create = new ArrayList<String>();
      ArrayList<String> write = new ArrayList<String>();
      ArrayList<String> read = new ArrayList<String>();
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
               if (CREATE_NAME.equals(type))
               {
                  create.add(role.trim());
               }
               else if (WRITE_NAME.equals(type))
               {
                  write.add(role.trim());
               }
               else if (READ_NAME.equals(type))
               {
                  read.add(role);
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
         securityRoles.add(new Role(role, read.contains(role), write.contains(role), create.contains(role)));
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
   public String[] getConfigFileNames()
   {
      return new String[] {QUEUES_XML};
   }
}

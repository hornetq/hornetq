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
package org.jboss.messaging.deployers.security;

import org.jboss.jms.server.security.Role;
import org.jboss.messaging.deployers.Deployer;
import org.jboss.messaging.util.HierarchicalRepository;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Deploys the security settings into a security repository and adds them to the security store.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class SecurityDeployer extends Deployer
{

   private static final String PERMISSION_ELEMENT_NAME = "permission";
   private static final String TYPE_ATTR_NAME = "type";
   private static final String ROLES_ATTR_NAME = "roles";
   private static final String QUEUES_XML = "queues.xml";
   private static final String MATCH = "match";
   private static final String SECURITY_ELEMENT_NAME = "security";

   /**
    * The repository to add to
    */
   private HierarchicalRepository<HashSet<Role>> securityRepository;

   /**
    * the names of the elements to deploy
    * @return the names of the elements todeploy
    */
   public String[] getElementTagName()
   {
      return new String[]{SECURITY_ELEMENT_NAME};
   }

   /**
    * the key attribute for theelement, usually 'name' but can be overridden
    * @return the key attribute
    */
   public String getKeyAttribute()
   {
      return MATCH;
   }

   public void setSecurityRepository(HierarchicalRepository<HashSet<Role>> securityRepository)
   {
      this.securityRepository = securityRepository;
   }

   /**
    * deploy an element
    * @param node the element to deploy
    * @throws Exception .
    */
   public void deploy(Node node) throws Exception
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
               if (Role.CREATE_NAME.equals(type))
               {
                  create.add(role);
               }
               else if (Role.WRITE_NAME.equals(type))
               {
                  write.add(role);
               }
               else if (Role.READ_NAME.equals(type))
               {
                  read.add(role);
               }
               if (!allRoles.contains(role))
                  allRoles.add(role);
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
    * @param node the element to undeploy
    * @throws Exception .
    */
   public void undeploy(Node node) throws Exception
   {
      String match = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
      securityRepository.removeMatch(match);
   }

   /**
    * The name of the configuration file name to look for for deployment
    *
    * @return The name of the config file
    */
   public String getConfigFileName()
   {
      return QUEUES_XML;
   }
}

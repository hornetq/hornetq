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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * This class is a state machine for failover state for a node.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class FailoverStatus implements Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -2668162690753929133L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // The set of nodes the server has completed failover for since it was last restarted
   private Set failedOverForNodes;

   // The node the server is currently failing over for (if any)
   private int currentlyFailingOverForNode;

   // Is the server currently failing over?
   private boolean failingOver;

   // Constructors --------------------------------------------------

   public FailoverStatus()
   {
      failedOverForNodes = new LinkedHashSet();
   }

   // Public --------------------------------------------------------

   public void startFailingOverForNode(Integer nodeID)
   {
      if (failingOver)
      {
         throw new IllegalStateException("Already failing over for node " + currentlyFailingOverForNode);
      }

      // Remove from failedOverNodes in case its failed over for the same node before.
      failedOverForNodes.remove(nodeID);
      
      currentlyFailingOverForNode = nodeID.intValue();
      failingOver = true;
   }

   public void finishFailingOver()
   {
      if (!failingOver)
      {
         throw new IllegalStateException("The node is not currently failing over");
      }

      failedOverForNodes.add(new Integer(currentlyFailingOverForNode));
      failingOver = false;
   }

   public Set getFailedOverForNodes()
   {
      return Collections.unmodifiableSet(failedOverForNodes);
   }

   public boolean isFailedOverForNode(int nodeId)
   {
      return failedOverForNodes.contains(new Integer(nodeId));
   }

   public boolean isFailingOverForNode(int nodeId)
   {
      return failingOver && currentlyFailingOverForNode == nodeId;
   }
   
   public boolean isFailingOver()
   {
      return failingOver;
   }

   public String toString()
   {
      return "FailoverStatus[" + currentlyFailingOverForNode + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

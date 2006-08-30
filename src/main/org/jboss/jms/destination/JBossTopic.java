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
package org.jboss.jms.destination;

import javax.jms.Topic;
import javax.jms.JMSException;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossTopic extends JBossDestination implements Topic
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 3257845497845724981L;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------     
   
   // Constructors --------------------------------------------------

   public JBossTopic(String name, int fullSize, int pageSize, int downCacheSize)
   {
      super(name, fullSize, pageSize, downCacheSize);
   }
   
   public JBossTopic(String name)
   {
      super(name);
   }

   // JBossDestination overrides ------------------------------------

   public boolean isTopic()
   {
      return true;
   }

   public boolean isQueue()
   {
      return false;
   }

   // Topic implementation ------------------------------------------

   public String getTopicName() throws JMSException
   {
      return getName();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "JBossTopic[" + name + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

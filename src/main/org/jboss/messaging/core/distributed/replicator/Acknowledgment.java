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
package org.jboss.messaging.core.distributed.replicator;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Acknowledgment implements Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -3953217132447293L;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable replicatorOutputID;
   protected Serializable messageID;
   protected boolean accepted;

   // Constructors --------------------------------------------------

   public Acknowledgment(Serializable replicatorOutputID, Serializable messageID, boolean accepted)
   {
      this.messageID = messageID;
      this.accepted = accepted;
      this.replicatorOutputID = replicatorOutputID;
   }

   // Public --------------------------------------------------------

   public Serializable getMessageID()
   {
      return messageID;
   }

   public boolean isAccepted()
   {
      return accepted;
   }

   public Serializable getReplicatorOutputID()
   {
      return replicatorOutputID;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

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

   public static final int REJECTED = 0;
   public static final int ACCEPTED = 1;
   public static final int CANCELLED = 2;

   private static final long serialVersionUID = -3953217132447293L;

   // Static --------------------------------------------------------

   public static String stateToString(int state)
   {
      return state == REJECTED ? "REJECTED" :
             state == ACCEPTED ? "ACCEPTED" :
             state == CANCELLED ? "CANCELLED" :
             "UNKNOWN";
   }

   // Attributes ----------------------------------------------------

   protected Serializable replicatorOutputID;
   protected Serializable messageID;
   protected int state;

   // Constructors --------------------------------------------------

   public Acknowledgment(Serializable replicatorOutputID, Serializable messageID, int state)
   {
      validateState(state);
      this.messageID = messageID;
      this.state = state;
      this.replicatorOutputID = replicatorOutputID;
   }

   // Public --------------------------------------------------------

   public Serializable getMessageID()
   {
      return messageID;
   }

   public int getState()
   {
      return state;
   }

   public Serializable getReplicatorOutputID()
   {
      return replicatorOutputID;
   }

   public String toString()
   {
      return "Acknowlegment[" + messageID + ", " + stateToString(state) + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private void validateState(int state)
   {
      if (state != REJECTED &&
          state != ACCEPTED &&
          state != CANCELLED)
      {
         throw new IllegalArgumentException("unknown state: " + state);
      }

   }

   // Inner classes -------------------------------------------------
}

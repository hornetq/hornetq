/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import javax.jms.JMSException;
import javax.resource.ResourceException;
import javax.resource.spi.LocalTransaction;

import org.jboss.messaging.core.logging.Logger;

/**
 * JMS Local transaction
 * 
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMLocalTransaction implements LocalTransaction
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMLocalTransaction.class);
   
   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The managed connection */
   private JBMManagedConnection mc;
  
   /**
    * Constructor
    * @param mc The managed connection
    */
   public JBMLocalTransaction(JBMManagedConnection mc)
   {
      if (trace)
         log.trace("constructor(" + mc + ")");

      this.mc = mc;
   }

   /**
    * Begin
    * @exception ResourceException Thrown if the operation fails
    */
   public void begin() throws ResourceException
   {
   }

   /**
    * Commit
    * @exception ResourceException Thrown if the operation fails
    */
   public void commit() throws ResourceException
   {
      mc.lock();
      try
      {
         if (mc.getSession().getTransacted())
            mc.getSession().commit();
      }
      catch (JMSException e)
      {
         throw new ResourceException("Could not commit LocalTransaction", e);
      }
      finally
      {
         mc.unlock();
      }
   }

   /**
    * Rollback
    * @exception ResourceException Thrown if the operation fails
    */
   public void rollback() throws ResourceException
   {
      mc.lock();
      try
      {
         if (mc.getSession().getTransacted())
            mc.getSession().rollback();
      }
      catch (JMSException ex)
      {
         throw new ResourceException("Could not rollback LocalTransaction", ex);
      }
      finally
      {
         mc.unlock();
      }
   }
}

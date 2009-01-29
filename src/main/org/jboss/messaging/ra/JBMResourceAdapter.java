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

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;

import org.jboss.messaging.core.logging.Logger;

/**
 * The resource adapter for JBoss Messaging
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMResourceAdapter implements ResourceAdapter
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMResourceAdapter.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** Hash code */
   private static final int HASH_CODE = 0x42;

   /** The bootstrap context */
   private BootstrapContext ctx;

   /**
    * Constructor
    */
   public JBMResourceAdapter()
   {
      if (trace)
         log.trace("constructor()");
   }

   public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) throws ResourceException
   {
      if (trace)
         log.trace("endpointActivation(" + endpointFactory + ", " + spec + ")");

      throw new NotSupportedException("Unsupported");
   }

   public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec spec)
   {
      if (trace)
         log.trace("endpointDeactivation(" + endpointFactory + ", " + spec + ")");
   }
   
   public XAResource[] getXAResources(ActivationSpec[] specs) throws ResourceException
   {
      if (trace)
         log.trace("getXAResources(" + specs + ")");

      throw new ResourceException("Unsupported");
   }
   
   public void start(BootstrapContext ctx) throws ResourceAdapterInternalException
   {
      if (trace)
         log.trace("start(" + ctx + ")");

      this.ctx = ctx;

      log.info("JBoss Messaging resource adapter started");
   }

   public void stop()
   {
      if (trace)
         log.trace("stop()");

      log.info("JBoss Messaging resource adapter stopped");
   }

   /**
    * Indicates whether some other object is "equal to" this one.
    * @param obj Object with which to compare
    * @return True if this object is the same as the obj argument; false otherwise.
    */
   public boolean equals(Object obj) {
      if (trace)
         log.trace("equals(" + obj + ")");

      if (!(obj instanceof JBMResourceAdapter)) {
         return false;
      }

      return true;
   }
   
   /**
    * Return the hash code for the object
    * @return The hash code
    */
   public int hashCode() {
      if (trace)
         log.trace("hashCode()");

      return HASH_CODE;
   }
}

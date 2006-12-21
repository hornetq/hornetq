/*
* JBoss, Home of Professional Open Source
* Copyright 2006, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.recovery;

import javax.transaction.xa.XAResource;

import org.jboss.logging.Logger;

import com.arjuna.ats.jta.recovery.XAResourceRecovery;

/**
 * JMS Provider Adapter based recovery.
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="juha@jboss.com">Juha Lindfors</a>
 *
 * @version $Revision: 1.1 $
 */
public class JMSProviderXAResourceRecovery implements XAResourceRecovery
{
   private boolean trace = log.isTraceEnabled();

	private static final Logger log = Logger.getLogger(JMSProviderXAResourceRecovery.class);

   /** The jms provider name */
   private String providerName;

   /** The delegate XAResource */
   private XAResourceWrapper wrapper;

   /** Whether the XAResource is working */
   private boolean working = false;

   public JMSProviderXAResourceRecovery()
   {
      if(trace)
			log.trace("Constructing JMSProviderXAResourceRecovery..");
   }

   public boolean initialise(String p)
   {
      if(trace)
			log.trace("Initialising JMSProviderXAResourceRecovery..");

      this.providerName = p;
      return true;
   }

   public boolean hasMoreResources()
   {
      // If the XAResource is already working
      if (working)
         return false;

      // Have we initialized yet?
      if (wrapper == null)
      {
         wrapper = new XAResourceWrapper();
         wrapper.setProviderName(providerName);
      }

      // Test the connection
      try
      {
         wrapper.getTransactionTimeout();
         working = true;
      }
      catch (Exception ignored)
      {
         //System.out.println(ignored.getMessage());
      }

      // This will return false until we get a successful connection
      return working;
   }

   public XAResource getXAResource()
   {
      return wrapper;
   }
}

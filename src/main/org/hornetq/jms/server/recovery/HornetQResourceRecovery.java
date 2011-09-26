/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
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
package org.hornetq.jms.server.recovery;

import org.jboss.tm.XAResourceRecovery;

import javax.transaction.xa.XAResource;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         9/20/11
 */
public class HornetQResourceRecovery implements XAResourceRecovery
{
   private final XARecoveryConfig config;

   public HornetQResourceRecovery(XARecoveryConfig config)
   {
      this.config = config;
   }

   public XAResource[] getXAResources()
   {
      return new XAResource[]{new HornetQXAResourceWrapper(config)};
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      HornetQResourceRecovery that = (HornetQResourceRecovery) o;

      if (config != null ? !config.equals(that.config) : that.config != null) return false;

      return true;
   }

   @Override
   public int hashCode()
   {
      return config != null ? config.hashCode() : 0;
   }
}

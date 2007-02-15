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
package org.jboss.test.messaging.tools;

import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.jms.jndi.JMSProviderAdapter;

/**
 * A TestJMSProviderAdaptor
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class TestJMSProviderAdaptor implements JMSProviderAdapter
{
   private static final long serialVersionUID = 8064613007110300720L;

   private Properties props;
   
   private String cfLookup;
   
   private String name;

   public TestJMSProviderAdaptor(Properties props, String cfLookup, String name)
   {
      this.props = props;
      
      this.cfLookup = cfLookup;
      
      this.name = name;
   }
   
   public String getFactoryRef()
   {
      return cfLookup;
   }

   public Context getInitialContext() throws NamingException
   {
      return new InitialContext(props);
   }

   public String getName()
   {
      return name;
   }

   public Properties getProperties()
   {
      return props;
   }

   public String getQueueFactoryRef()
   {
      return cfLookup;
   }

   public String getTopicFactoryRef()
   {
      return cfLookup;
   }

   public void setFactoryRef(String lookup)
   {
      this.cfLookup = lookup;
   }

   public void setName(String name)
   {
      this.name = name;
   }

   public void setProperties(Properties props)
   {
      this.props = props;
   }

   public void setQueueFactoryRef(String lookup)
   {
      this.cfLookup = lookup;
   }

   public void setTopicFactoryRef(String lookup)
   {
      this.cfLookup = lookup;
   }


}


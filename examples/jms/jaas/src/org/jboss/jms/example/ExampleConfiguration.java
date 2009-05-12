/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.jms.example;

import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/**
 * A ExampleConfiguration
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ExampleConfiguration extends Configuration
{
   private final Map<String, ?> options;
   
   private final String loginModuleName;

   public ExampleConfiguration(String loginModuleName, Map<String, ?> options)
   {
      this.loginModuleName = loginModuleName;
      this.options = options;
   }

   @Override
   public AppConfigurationEntry[] getAppConfigurationEntry(final String name)
   {
      AppConfigurationEntry entry = new AppConfigurationEntry(loginModuleName,
                                                              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                                              options);
      return new AppConfigurationEntry[]{entry};
   }

   @Override
   public void refresh()
   {
   }
}

/*
 * Copyright 2010 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.config;

import java.io.Serializable;
import java.util.Map;

/**
 * A ConnectorServiceConfiguration
 *
 * @author <a href="tm.igarashi@gmail.com">Tomohisa Igarashi</a>
 *
 *
 */
public class ConnectorServiceConfiguration implements Serializable
{
   private static final long serialVersionUID = -641207073030767325L;

   private final String name;

   private final  String factoryClassName;

   private final  Map<String, Object> params;

   public ConnectorServiceConfiguration(final String clazz, final Map<String, Object> params, final String name)
   {
      this.name = name;
      factoryClassName = clazz;
      this.params = params;
   }

   public String getConnectorName()
   {
      return name;
   }

   public String getFactoryClassName()
   {
      return factoryClassName;
   }

   public Map<String, Object> getParams()
   {
      return params;
   }
}

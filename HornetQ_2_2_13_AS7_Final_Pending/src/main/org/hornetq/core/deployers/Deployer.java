/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.deployers;

import java.net.URL;

import org.hornetq.core.server.HornetQComponent;

/**
 * abstract class that helps with deployment of messaging components.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface Deployer extends HornetQComponent
{
   /**
    * The name of the configuration files to look for for deployment
    *
    * @return The names of the config files
    */
   String[] getConfigFileNames();

   /**
    * Deploy the URL for the first time
    *
    * @param url The resource todeploy
    * @throws Exception .
    */
   void deploy(URL url) throws Exception;

   /**
    * Redeploys a URL if changed
    *
    * @param url The resource to redeploy
    * @throws Exception .
    */
   void redeploy(URL url) throws Exception;

   /**
    * Undeploys a resource that has been removed
    * @param url The Resource that was deleted
    * @throws Exception .
    */
   void undeploy(URL url) throws Exception;
}
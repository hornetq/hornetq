/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.deployers;

import java.net.URL;

import org.jboss.messaging.core.server.MessagingComponent;

/**
 * abstract class that helps with deployment of messaging components.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public interface Deployer extends MessagingComponent
{
   /**
    * The name of the configuration file name to look for for deployment
    *
    * @return The name of the config file
    */
   String getConfigFileName();

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
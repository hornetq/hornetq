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
package org.jboss.jms.server;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import java.util.Set;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.aop.AspectXmlLoader;
import org.jboss.jms.server.connectionfactory.ConnectionFactoryJNDIMapper;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.jms.server.connectormanager.SimpleConnectorManager;
import org.jboss.jms.server.endpoint.ServerConsumerEndpoint;
import org.jboss.jms.server.plugin.contract.ChannelMapper;
import org.jboss.jms.server.plugin.contract.ThreadPool;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.jms.server.remoting.JMSWireFormat;
import org.jboss.jms.server.security.SecurityMetadataStore;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.messaging.util.Util;
import org.jboss.mx.loading.UnifiedClassLoader3;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.marshal.MarshalFactory;
import org.jboss.remoting.serialization.SerializationStreamFactory;
import org.jboss.system.ServiceCreator;
import org.jboss.system.ServiceMBeanSupport;
import org.w3c.dom.Element;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * A bootstrap for a scoped deployment.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Bootstrap extends ServiceMBeanSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Bootstrap.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public Bootstrap() throws Exception
   {
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

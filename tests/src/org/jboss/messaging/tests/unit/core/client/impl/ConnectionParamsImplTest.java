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

package org.jboss.messaging.tests.unit.core.client.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A ConnectionParamsImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConnectionParamsImplTest extends UnitTestCase
{
   
   public void testDefaults()
   {
      ConnectionParams cp = new ConnectionParamsImpl();
      
      assertEquals(ConnectionParamsImpl.DEFAULT_INVM_OPTIMISATION_ENABLED, cp.isInVMOptimisationEnabled());
      assertEquals(ConnectionParamsImpl.DEFAULT_SSL_ENABLED, cp.isSSLEnabled());
      assertEquals(ConnectionParamsImpl.DEFAULT_TCP_NODELAY, cp.isTcpNoDelay());
      assertEquals(ConnectionParamsImpl.DEFAULT_CALL_TIMEOUT, cp.getCallTimeout());
      assertEquals(ConnectionParamsImpl.DEFAULT_PING_INTERVAL, cp.getPingInterval());
      assertEquals(ConnectionParamsImpl.DEFAULT_TCP_RECEIVE_BUFFER_SIZE, cp.getTcpReceiveBufferSize());
      assertEquals(ConnectionParamsImpl.DEFAULT_TCP_SEND_BUFFER_SIZE, cp.getTcpSendBufferSize());
      assertEquals(null, cp.getKeyStorePath());
      assertEquals(null, cp.getKeyStorePassword());
      assertEquals(null, cp.getTrustStorePath());
      assertEquals(null, cp.getTrustStorePassword());
   }
      
   public void testSetAndGetAttributes()
   {
      for (int j = 0; j < 100; j++)
      {
         ConnectionParams cp = new ConnectionParamsImpl();
         
         boolean b = RandomUtil.randomBoolean();
         cp.setInVMOptimisationEnabled(b);
         assertEquals(b, cp.isInVMOptimisationEnabled());
         
         b = RandomUtil.randomBoolean();
         cp.setSSLEnabled(b);
         assertEquals(b, cp.isSSLEnabled());
         
         b = RandomUtil.randomBoolean();
         cp.setTcpNoDelay(b);
         assertEquals(b, cp.isTcpNoDelay());
         
         int i = RandomUtil.randomInt();
         cp.setCallTimeout(i);
         assertEquals(i, cp.getCallTimeout());
         
         long l = RandomUtil.randomLong();
         cp.setPingInterval(l);
         assertEquals(l, cp.getPingInterval());
         
         i = RandomUtil.randomInt();
         cp.setTcpReceiveBufferSize(i);
         assertEquals(i, cp.getTcpReceiveBufferSize());
         
         i = RandomUtil.randomInt();
         cp.setTcpSendBufferSize(i);
         assertEquals(i, cp.getTcpSendBufferSize());
         
         String s = RandomUtil.randomString();
         cp.setKeyStorePath(s);
         assertEquals(s, cp.getKeyStorePath());
         
         s = RandomUtil.randomString();
         cp.setKeyStorePassword(s);
         assertEquals(s, cp.getKeyStorePassword());
         
         s = RandomUtil.randomString();
         cp.setTrustStorePath(s);
         assertEquals(s, cp.getTrustStorePath());
         
         s = RandomUtil.randomString();
         cp.setTrustStorePassword(s);
         assertEquals(s, cp.getTrustStorePassword());   
      }      
   }
   
   public void testOverrideWithSystemProperties()
   {
      ConnectionParams cp = new ConnectionParamsImpl();
      
      try
      {      
         assertEquals(ConnectionParamsImpl.DEFAULT_SSL_ENABLED, cp.isSSLEnabled());     
         System.setProperty(ConnectionParamsImpl.ENABLE_SSL_PROPERTY_NAME, String.valueOf(!ConnectionParamsImpl.DEFAULT_SSL_ENABLED));      
         assertEquals(!ConnectionParamsImpl.DEFAULT_SSL_ENABLED, cp.isSSLEnabled());     
         
         assertEquals(null, cp.getKeyStorePath());      
         final String path = "somepath";
         System.setProperty(ConnectionParamsImpl.SSL_KEYSTORE_PATH_PROPERTY_NAME, path);
         assertEquals(path, cp.getKeyStorePath());
         
         assertEquals(null, cp.getKeyStorePassword());      
         final String password = "somepassword";
         System.setProperty(ConnectionParamsImpl.SSL_KEYSTORE_PASSWORD_PROPERTY_NAME, password);
         assertEquals(password, cp.getKeyStorePassword());
         
         assertEquals(null, cp.getTrustStorePath());      
         final String trustpath = "sometrustpath";
         System.setProperty(ConnectionParamsImpl.SSL_TRUSTSTORE_PATH_PROPERTY_NAME, trustpath);
         assertEquals(trustpath, cp.getTrustStorePath());
         
         assertEquals(null, cp.getTrustStorePassword());      
         final String trustpassword = "sometrustpassword";
         System.setProperty(ConnectionParamsImpl.SSL_TRUSTSTORE_PASSWORD_PROPERTY_NAME, trustpassword);
         assertEquals(trustpassword, cp.getTrustStorePassword());
      }
      finally
      {
         System.clearProperty(ConnectionParamsImpl.ENABLE_SSL_PROPERTY_NAME);
         System.clearProperty(ConnectionParamsImpl.SSL_KEYSTORE_PATH_PROPERTY_NAME);
         System.clearProperty(ConnectionParamsImpl.SSL_KEYSTORE_PASSWORD_PROPERTY_NAME);
         System.clearProperty(ConnectionParamsImpl.SSL_TRUSTSTORE_PATH_PROPERTY_NAME);
         System.clearProperty(ConnectionParamsImpl.SSL_TRUSTSTORE_PASSWORD_PROPERTY_NAME);
      }
   }
   
   public void testSerialize() throws Exception
   {
      ConnectionParams cp = new ConnectionParamsImpl();
      
      boolean b = RandomUtil.randomBoolean();
      cp.setInVMOptimisationEnabled(b);

      b = RandomUtil.randomBoolean();
      cp.setSSLEnabled(b);

      b = RandomUtil.randomBoolean();
      cp.setTcpNoDelay(b);
   
      int i = RandomUtil.randomInt();
      cp.setCallTimeout(i);
  
      long l = RandomUtil.randomLong();
      cp.setPingInterval(l);
      
      i = RandomUtil.randomInt();
      cp.setTcpReceiveBufferSize(i);

      i = RandomUtil.randomInt();
      cp.setTcpSendBufferSize(i);

      String s = RandomUtil.randomString();
      cp.setKeyStorePath(s);
     
      s = RandomUtil.randomString();
      cp.setKeyStorePassword(s);
 
      s = RandomUtil.randomString();
      cp.setTrustStorePath(s);
  
      s = RandomUtil.randomString();
      cp.setTrustStorePassword(s);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(cp);
      oos.flush();
      
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bais);
      ConnectionParamsImpl cp2 = (ConnectionParamsImpl)ois.readObject();
      
      assertTrue(cp.equals(cp2));      
   }
   
   // Private -----------------------------------------------------------------------------------------------------------

}

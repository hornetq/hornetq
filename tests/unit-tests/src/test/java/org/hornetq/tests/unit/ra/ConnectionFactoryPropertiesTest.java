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

package org.hornetq.tests.unit.ra;

import org.junit.Test;

import static java.beans.Introspector.getBeanInfo;

import java.beans.PropertyDescriptor;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.tests.util.UnitTestCase;

public class ConnectionFactoryPropertiesTest extends UnitTestCase {

    private static final SortedSet<String> UNSUPPORTED_CF_PROPERTIES;
    private static final SortedSet<String> UNSUPPORTED_RA_PROPERTIES;

    static {
       UNSUPPORTED_CF_PROPERTIES = new TreeSet<String>();
       UNSUPPORTED_CF_PROPERTIES.add("discoveryGroupName");

       UNSUPPORTED_RA_PROPERTIES = new TreeSet<String>();
       UNSUPPORTED_RA_PROPERTIES.add("HA");
       UNSUPPORTED_RA_PROPERTIES.add("jgroupsChannelName");
       UNSUPPORTED_RA_PROPERTIES.add("jgroupsFile");
       UNSUPPORTED_RA_PROPERTIES.add("discoveryAddress");
       UNSUPPORTED_RA_PROPERTIES.add("discoveryPort");
       UNSUPPORTED_RA_PROPERTIES.add("discoveryLocalBindAddress");
       UNSUPPORTED_RA_PROPERTIES.add("discoveryRefreshTimeout");
       UNSUPPORTED_RA_PROPERTIES.add("discoveryInitialWaitTimeout");
       UNSUPPORTED_RA_PROPERTIES.add("connectionParameters");
       UNSUPPORTED_RA_PROPERTIES.add("connectorClassName");
       UNSUPPORTED_RA_PROPERTIES.add("transactionManagerLocatorClass");
       UNSUPPORTED_RA_PROPERTIES.add("transactionManagerLocatorMethod");
       UNSUPPORTED_RA_PROPERTIES.add("managedConnectionFactory");
       UNSUPPORTED_RA_PROPERTIES.add("jndiParams");
       UNSUPPORTED_RA_PROPERTIES.add("password");
       UNSUPPORTED_RA_PROPERTIES.add("passwordCodec");
       UNSUPPORTED_RA_PROPERTIES.add("useMaskedPassword");
       UNSUPPORTED_RA_PROPERTIES.add("useAutoRecovery");
       UNSUPPORTED_RA_PROPERTIES.add("useLocalTx");
       UNSUPPORTED_RA_PROPERTIES.add("userName");
       UNSUPPORTED_RA_PROPERTIES.add("jgroupsChannelLocatorClass");
       UNSUPPORTED_RA_PROPERTIES.add("jgroupsChannelRefName");

       // TODO: shouldn't this be also set on the HornetQConnectionFactory:
       // https://community.jboss.org/thread/211815?tstart=0
       UNSUPPORTED_RA_PROPERTIES.add("connectionPoolName");
    }

   @Test
    public void testCompareConnectionFactoryAndResourceAdapterProperties() throws Exception {
        SortedSet<String> connectionFactoryProperties = findAllPropertyNames(HornetQConnectionFactory.class);
        connectionFactoryProperties.removeAll(UNSUPPORTED_CF_PROPERTIES);
        SortedSet<String> raProperties = findAllPropertyNames(HornetQResourceAdapter.class);
        raProperties.removeAll(UNSUPPORTED_RA_PROPERTIES);

        compare("HornetQ Connection Factory", connectionFactoryProperties,
              "HornetQ Resource Adapter", raProperties);
    }

    private static void compare(String name1, SortedSet<String> set1,
            String name2, SortedSet<String> set2) {
        Set<String> onlyInSet1 = new TreeSet<String>(set1);
        onlyInSet1.removeAll(set2);

        Set<String> onlyInSet2 = new TreeSet<String>(set2);
        onlyInSet2.removeAll(set1);

        if (!onlyInSet1.isEmpty() || !onlyInSet2.isEmpty()) {
            fail(String.format("in %s only: %s\nin %s only: %s", name1, onlyInSet1, name2, onlyInSet2));
        }

        assertEquals(set2, set1);
    }

    private SortedSet<String> findAllPropertyNames(Class<?> clazz) throws Exception {
        SortedSet<String> names = new TreeSet<String>();
        for (PropertyDescriptor propDesc : getBeanInfo(clazz).getPropertyDescriptors()) {
            if (propDesc == null
                || propDesc.getWriteMethod() == null) {
                continue;
            }
            names.add(propDesc.getDisplayName());
        }
        return names;
    }
}
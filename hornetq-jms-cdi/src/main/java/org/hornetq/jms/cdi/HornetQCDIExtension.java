/*
 * Copyright 2013 Red Hat, Inc.
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

package org.hornetq.jms.cdi;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.InjectionTarget;
import javax.enterprise.util.AnnotationLiteral;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSPasswordCredential;
import javax.jms.JMSSessionMode;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.hornetq.jms.client.HornetQJMSContext;

/**
 * @author <a href="http://jmesnil.net/">Jeff Mesnil</a> (c) 2013 Red Hat inc.
 */
public class HornetQCDIExtension implements Extension
{

   void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm)
   {
      //use this to read annotations of the class
      final AnnotatedType<JMSContext> at = bm.createAnnotatedType(JMSContext.class);

      // FIXME how do I create a JMSContext using the JMSContextGenerator producer method?
      final InjectionTarget<JMSContext> it = bm.createInjectionTarget(at);

      abd.addBean(new Bean<JMSContext>()
      {
         @Override
         public Set<Type> getTypes()
         {
            Set<Type> types = new HashSet<Type>();
            types.add(JMSContext.class);
            types.add(Object.class);
            return types;
         }

         @Override
         public Set<Annotation> getQualifiers()
         {
            Set<Annotation> qualifiers = new HashSet<Annotation>();
            qualifiers.add(new AnnotationLiteral<Default>() {});
            qualifiers.add(new AnnotationLiteral<Any>() {});
            return qualifiers;
         }

         @Override
         public Class<? extends Annotation> getScope()
         {
            return ApplicationScoped.class;
         }

         @Override
         public String getName()
         {
            return "jmsContext";
         }

         @Override
         public Set<Class<? extends Annotation>> getStereotypes()
         {
            return Collections.emptySet();
         }

         @Override
         public Class<?> getBeanClass()
         {
            return HornetQJMSContext.class;
         }

         @Override
         public boolean isAlternative()
         {
            return false;
         }

         @Override
         public boolean isNullable()
         {
            return false;
         }

         @Override
         public Set<InjectionPoint> getInjectionPoints()
         {
            return it.getInjectionPoints();
         }

         @Override
         public JMSContext create(CreationalContext<JMSContext> creationalContext)
         {
            // FIXME should be using it's injectionPoints
            System.out.println("########## it.getInjectionPoints()" + it.getInjectionPoints());
            System.out.println("########## at.getAnnotations() = " + at.getAnnotations());
            String userName = null;
            String password = null;
            JMSPasswordCredential passwordCredential = at.getAnnotation(JMSPasswordCredential.class);
            if (passwordCredential != null)
            {
               userName = passwordCredential.userName();
               password = passwordCredential.password();
            }
            int ackMode = JMSContext.AUTO_ACKNOWLEDGE;
            JMSSessionMode sessionMode = at.getAnnotation(JMSSessionMode.class);
            if (sessionMode != null)
            {
               ackMode = sessionMode.value();
            }
            // FIXME define the default JMS connection factory
            String cfLookup = "jms/connectionFactory";
            JMSConnectionFactory jmscf = at.getAnnotation(JMSConnectionFactory.class);
            if (jmscf != null)
            {
               cfLookup = jmscf.value();
            }

            ConnectionFactory cf = null;
            try
            {
               InitialContext jndiCtx = new InitialContext();
               cf = (ConnectionFactory) jndiCtx.lookup(cfLookup);
            } catch (NamingException e)
            {
               e.printStackTrace();
            }
            JMSContext instance = new HornetQJMSContext(cf, ackMode, userName, password);
            return instance;
         }

         @Override
         public void destroy(JMSContext instance, CreationalContext<JMSContext> creationalContext)
         {
            instance.close();
            creationalContext.release();
         }
      });
   }
}

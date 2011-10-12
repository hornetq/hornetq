package org.hornetq.rest.queue.push;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.core.logging.Logger;
import org.hornetq.rest.queue.push.xml.BasicAuth;
import org.hornetq.rest.queue.push.xml.PushRegistration;
import org.hornetq.rest.queue.push.xml.XmlHttpHeader;
import org.hornetq.rest.util.HttpMessageHelper;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.client.core.executors.ApacheHttpClientExecutor;
import org.jboss.resteasy.specimpl.UriBuilderImpl;

import javax.ws.rs.core.UriBuilder;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class UriStrategy implements PushStrategy
{
   private static final Logger log = Logger.getLogger(UriStrategy.class);
   protected HttpClient client = new HttpClient();
   protected ApacheHttpClientExecutor executor = new ApacheHttpClientExecutor(client);
   protected PushRegistration registration;
   protected UriBuilder targetUri;
   protected String method;
   protected String contentType;

   public void setRegistration(PushRegistration reg)
   {
      this.registration = reg;
   }

   public void start() throws Exception
   {
      initAuthentication();
      method = registration.getTarget().getMethod();
      if (method == null) method = "POST";
      contentType = registration.getTarget().getType();
      targetUri = UriBuilderImpl.fromTemplate(registration.getTarget().getHref());
   }

   protected void initAuthentication()
   {
      if (registration.getAuthenticationMechanism() != null)
      {
         if (registration.getAuthenticationMechanism().getType() instanceof BasicAuth)
         {
            BasicAuth basic = (BasicAuth) registration.getAuthenticationMechanism().getType();
            //log.info("Setting Basic Auth: " + basic.getUsername());
            client.getParams().setAuthenticationPreemptive(true);
            client.getState().setCredentials(
                    //new AuthScope(null, 8080, "Test"),
                    new AuthScope(AuthScope.ANY),
                    new UsernamePasswordCredentials(basic.getUsername(), basic.getPassword())
            );
         }
      }
   }

   public void stop()
   {
   }

   public boolean push(ClientMessage message)
   {
      String uri = createUri(message);
      for (int i = 0; i < registration.getMaxRetries(); i++)
      {
         long wait = registration.getRetryWaitMillis();
         ClientRequest request = executor.createRequest(uri);
         request.followRedirects(false);

         for (XmlHttpHeader header : registration.getHeaders())
         {
            request.header(header.getName(), header.getValue());
         }
         HttpMessageHelper.buildMessage(message, request, contentType);
         ClientResponse res = null;
         try
         {
            log.debug(method + " " + uri);
            res = request.httpMethod(method);
            int status = res.getStatus();
            if (status == 503)
            {
               String retryAfter = (String) res.getHeaders().getFirst("Retry-After");
               if (retryAfter != null)
               {
                  wait = Long.parseLong(retryAfter) * 1000;
               }
            }
            else if (status == 307)
            {
               uri = res.getLocation().getHref();
               wait = 0;
            }
            else if ((status >= 200 && status < 299) || status == 303 || status == 304)
            {
               log.debug("Success");
               return true;
            }
            else if (status >= 400)
            {
               switch (status)
               {
                  case 400: // these usually mean the message you are trying to send is crap, let dead letter logic take over
                  case 411:
                  case 412:
                  case 413:
                  case 414:
                  case 415:
                  case 416:
                     throw new RuntimeException("Something is wrong with the message, status returned: " + status + " for push registration of URI: " + uri);
                  case 401: // might as well consider these critical failures and abort.  Immediately signal to disable push registration depending on config
                  case 402:
                  case 403:
                  case 405:
                  case 406:
                  case 407:
                  case 417:
                  case 505:
                     return false;
                  case 404:  // request timeout, gone, and not found treat as a retry
                  case 408:
                  case 409:
                  case 410:
                     break;
                  default: // all 50x requests just retry (except 505)
                     break;
               }
            }
         }
         catch (Exception e)
         {
            //throw new RuntimeException(e);
         }
         try
         {
            if (wait > 0) Thread.sleep(wait);
         }
         catch (InterruptedException e)
         {
            throw new RuntimeException("Interrupted");
         }
      }
      return false;
   }

   protected String createUri(ClientMessage message)
   {
      String uri = targetUri.build().toString();
      return uri;
   }
}

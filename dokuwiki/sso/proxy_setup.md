[SSO Pathfinder Single Sign On](https://mvp.developer.gov.bc.ca/docs/default/component/css-docs/) provides an easy integration for IDIR authentication available to all BC government digital products. This service provides developers with the tools needed to quickly deploy IDIR authentication for your website.

To begin this process, review the [SSO onboarding instructions](https://mvp.developer.gov.bc.ca/docs/default/component/css-docs/SSO-Onboarding/), and make a request for the necessary credentials in the [Common hosted Single Sign on CSS App](https://bcgov.github.io/sso-requests/).

After provisioning your SSO details, you are ready to implement an SSO authentication process in your application. You can create your own application, or continue to follow the instructions below to deploy authentication using the [SSO Pathfinder Confidential Client Using Node-Express](https://github.com/bcgov/km-sso-client).

## SSO Pathfinder Confidential Client

The [SSO Pathfinder Confidential Client Using Node-Express](https://github.com/bcgov/km-sso-client) authenticates users using OAuth 2.0 standard flow.

To deploy this application on openshift, you will need three components, deployed from bitnami packages with custom values.yaml files:

1. An Nginx Reverse Proxy [Bitnami Nginx](https://github.com/bitnami/charts/tree/main/bitnami/nginx)
2. The SSO client [Bitnami Node.js](https://github.com/bitnami/containers/tree/main/bitnami/node)
3. A Redis Session Management Data Store [Bitnami Redis](https://github.com/bitnami/charts/tree/main/bitnami/redis)

On all helm charts, ensure that you are updating the resources requested as appropriate for your namespace and application needs.

Custom values provided are examples, and may need to be updated to meet the needs of your application.

### 1. Nginx Reverse Proxy

Deploy the NGINX Reverse Proxy using a custom `values.yaml`:

`helm install proxy -f values.yaml oci://registry-1.docker.io/bitnamicharts/nginx`

**Custom values:**

`values.yaml`

(Use or delete serverBlock components as required)

```
containerPorts:
    https: 8081
service:
    type: ClusterIP
serverBlock: |-
  server {
    listen 8081;
    listen [::]:8081;
    server_name <host>; (ex: test.apps.silver.devops.gov.bc.ca)

    error_page 401 403 = @login_required;

    # define nginx caching flags
    set $bypass_cache 0;
    set $no_cache 0;
    set $skip_reason "None";
    set $purge_cache "None";
    set $purge_reason "None";

    # Anyone with the link can access this. Bypasses /auth/.
    # Proxy pass all embedded requests.
    # add_header Alows iframe embedding.
    # regex compatible location routing
    location ~ ^/[0-9a-z-]+/ {
      proxy_pass http://<service>:<PORT>$request_uri;
      proxy_set_header Host $host;
      proxy_set_header X-Original-URI $request_uri;
      add_header Content-Security-Policy "frame-src 'self' <URL>;";
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
      proxy_set_header X-Forwarded-Proto https;
      proxy_set_header X-Real-IP $remote_addr;
      proxy_set_header X-Forwarded-Uri /rule;
      proxy_read_timeout 300s;
      proxy_connect_timeout 75s;
    }

    # anyone with IDIR identity can access this
    # API
    location /api/ {
      auth_request /auth/;
      auth_request_set $auth_status $upstream_status;
      proxy_set_header Host $host;
      proxy_set_header X-Original-URI $request_uri;
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
      proxy_set_header X-Forwarded-Proto https;
      proxy_set_header X-Real-IP $remote_addr;
      proxy_read_timeout 300s;
      proxy_connect_timeout 75s;
      proxy_pass http://<service>:<PORT>/;
    }

    # anyone with IDIR identity can access this
    # Root URL
    location / {
      auth_request /auth/;
      auth_request_set $auth_status $upstream_status;
      proxy_set_header Host $host;
      proxy_set_header X-Original-URI $request_uri;
      proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
      proxy_set_header X-Forwarded-Proto https;
      proxy_set_header X-Real-IP $remote_addr;
      proxy_read_timeout 300s;
      proxy_connect_timeout 75s;
      proxy_pass http://<service>/;
    }

    # authenticate/authorize user
    location /auth/ {
        internal;
        proxy_pass_request_body off;
        proxy_set_header Content-Length "";
        proxy_set_header Host $http_host;
        proxy_set_header X-Original-URI $request_uri;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header Authorization $http_authorization;
        proxy_pass_header Authorization;
        proxy_pass http://sso-client-node/;
      }

      location /sso-login {
        proxy_set_header Host $host;
        proxy_set_header X-Original-URI $request_uri;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_pass http://sso-client-node/authn;
      }

      location /sso {
        proxy_set_header Host $host;
        proxy_set_header X-Original-URI $request_uri;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_pass http://sso-client-node/authn/callback;
      }

      location @login_required {
        return 302 https://<hostURL>/sso-login?relay=$request_uri;
      }
  }
```

### 2. Custom SSO Client

Deploy the SSO client using a custom `values.yaml`:

1. Create a secret in the namespace with the following values.

**Secret Name**: dev-sso-client-conf

```
  SSO_BASE_URL: <BASEWEBSITEURL>
  SSO_CLIENT_ID: <CSS APP - 'resource'>
  SSO_CLIENT_SECRET: <CSS APP - 'credentials.secret'>
  SSO_LOGOUT_REDIRECT_URI: http://gov.bc.ca
  SSO_REDIS_SESSION_STORE_URL: redis://session-store-redis-master:6379
  SSO_AUTH_SERVER_URL: <CSS APP - 'auth-server-url'>
  SSO_REDIRECT_URL: <BASEWEBSITEURL>/sso
  SSO_REALM: standard
  SSO_SESSION_SECRET: <Redis Session Secret>
```

Pull the necessary values for the SSO_CLIENT_SECRET, SSO_CLIENT_ID, SSO_AUTH_SERVER_URL from the values generated by the [Common hosted Single Sign on CSS App](https://bcgov.github.io/sso-requests/).

2. `helm install sso-client -f values.yaml bitnami/node`

**Custom values:**

`values.yaml`

```
extraEnvVars:
 - name: SSO_REDIS_CONNECT_PASSWORD
   valueFrom:
    secretKeyRef:
      key: redis-password
      name: session-store-redis
extraEnvVarsSecret: "dev-sso-client-conf"
mongodb:
  enabled: false
livenessProbe:
  path: '/health'
  initialDelaySeconds: 20
readinessProbe:
  path: '/health'
  initialDelaySeconds: 20
repository: 'https://github.com/bcgov/km-sso-client'
revision: main
persistence:
  mountPath: /data
  accessModes:
    - ReadWriteMany
service:
  port: 3000
```

### 3. Redis Session Management Data Store

Deploy the Redis Session Store using a custom `values.yaml`:

`helm install session-store -f values.yaml oci://registry-1.docker.io/bitnamicharts/redis`

**Example Custom values:**

`values.yaml`

```
replica:
  replicaCount: 1
networkPolicy:
  enabled: false
```

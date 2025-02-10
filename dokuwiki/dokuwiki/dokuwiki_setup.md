[AMPs documentation regarding doolittle and how it was setup with dokuwiki](https://dev.azure.com/bc-icm/FODIG/_wiki/wikis/FODIG.wiki/177/Doolittle). AMP currently uses a bitnami helm chart. The bitnami chart is now deprecated, though.

1. [Install the oc command line tool](https://developer.gov.bc.ca/docs/default/component/platform-developer-docs/docs/openshift-projects-and-access/install-the-oc-command-line-tool/)
    1. Run cmd as admin: `wsl --install`
    1. Turn off VPN and Open Ubantu (WSL) terminal
    2. install homebrew: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
    3. instal oc: `brew install openshift-cli`
    4. get login token from OpenShift console by clicking the button under your username.
2. In you Ubantu (WSL) terminal, `cd` into the directory with the `values.yml` file that will be used to do a custom install of the dokuwiki
    * You need to put `/mnt/` in front of the local directory. E.g. to get to C:/documents, run `cd /mnt/c/documents`
    * make sure the values you set are the values you want for your custom installation
3. Install the dokuwiki: `helm install yourcustomname -f values.yaml oci://registry-1.docker.io/bitnamicharts/dokuwiki --namespace youropenshiftnamespace`
4. Follow the release notes after install
5. In openshift, create a route, secret for the admin passowrd, and persistent volume claim
 
* If an nginx proxy hasn't been deployed for your namespace to handle authentication and routing yet, the AMP wiki has a [guide for this](https://dev.azure.com/bc-icm/FODIG/_wiki/wikis/FODIG.wiki/280/SSO-(IDIR-Auth)). This guide is copied in this repo dir at sso/proxy_setup.md

* Backups can be installed using AMPs [custom dokuwiki backup tool](https://github.com/bcgov/km-dokuwiki-backup) or they can be managed manually. AMPs backup tool has been adapted for the purposes of the SDPR CDW DokuWiki [here](https://github.com/bcgov/sdpr-cdw-helm-charts/tree/main/charts/dokuwiki-backup).
Doolittle is a centralized wiki platform for organizing, managing, and sharing knowledge related to Information Services Division (ISD) integrated systems.

Doolittle uses [Dokuwiki](https://www.dokuwiki.org/), an open source wiki application written in the PHP. It works on plain text files and does not require a database. Its syntax is similar to the one used by MediaWiki.

# Domains

| Domain                          | Namespace   | Description                                                             |
| ------------------------------- | ----------- | ----------------------------------------------------------------------- |
| knowledge.social.gov.bc.ca      | aebbdd-prod | ISD knowledge management portal (Doolittle)                             |
| dev.knowledge.social.gov.bc.ca  | aebbdd-dev  | Development environment for ISD knowledge management portal (Doolittle) |
| test.knowledge.social.gov.bc.ca | aebbdd-test | Staging environment for ISD knowledge management portal (Doolittle)     |

# Maintenance

## Dokuwiki Releases

[Dokuwiki Helm Chart (Bitnami)](https://github.com/bitnami/charts/tree/main/bitnami/dokuwiki/)

## Error logs

DokuWiki logs errors that can be viewed via the admin interface called LogViewer. If the LogViewer cannot reached, you can find the log files in `[wiki_folder]/data/log/error/<date>.log`.

If error logs are enabled, they can accumulate and use up storage volume capacity. Use the following command to view storage usage of error logs:

`du -c -h /bitnami/dokuwiki/data/log | sort -n -r | head -n 20`

# Backups

https://github.com/bcgov/km-dokuwiki-backup

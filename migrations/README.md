OpenLedger database migrations are applied by the one-shot `migrate` Compose
service before the web application and worker start. Never edit an applied
migration; add a new revision instead.

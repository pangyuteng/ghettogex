
celery
   - easy to scale jobs later
   - crontab can trigger tasks down to second level
   - db maintaince
luigi
    - use to enusre 1 task in kube
    - later can do backfill jobs via celery crontab
timescaledb
    - postgres was working fine
    - query speed up via index
    - postgres got too slow as db increase
    - had to make partition, using timescaledb hypertable is easier
    - leveraging time_bucket in timescaledb
psycopg3,asyncio
    - row insert via single one-conn,one-cur,one-execute too slow
      above likely causing events data to no sync up - quote vs timeandsale price
      switched to using pipeline and implement "buffer/flush","producer,queue,consumer" logic with asyncio Queue,Event.
      also now using "pipeline" feature.
tastyware
    - super nice with async and psycopg3
    - updated version to leverate refresh_interval at each event subscription
    - greeks event only updated 1minute
pyvollib_vectorized
    - use to comptue price,iv,greeks
ssvi,svi
    - TODO: maybe need to construct iv surface, to better guess DDOI
quart
    - simple lib to serve uplot and use ws to get data from db.
uplot
    - super fast, 
    - plotly was too slow
longhorn
    - todo, maybe use to store json?
    - maybe test out db performance
kubernetes
    - kubectl exe
cloudflared
    - expose to public web
ansible
    - make vm os update more manageble






# hola ghettogex

```

this is a repo used by me to understand option greeks exposure.

+ main page `/` shows you SPX price, volatility, VIX, and pm 0dte contract volume.

+ spx pm 0dte gex,dex is displayed in `/scratch`. this is ** garbage and unreliable **

*** DISCLAIMER ***

+ other than price and volume streamed from the data vendor, treat everything else computed by this repo as garbage and unreliable.

+ repo content is provided for educational purposes only.

+ for more disclaimer & info see `/about`.

+ i reserve all right to not respond to any issues.

```


## obtain tastytrade OAuth client secret and refesh token

+ refer to instructions here: https://tastyworks-api.readthedocs.io/en/latest/sessions.html#creating-an-oauth-application

## deployment

#### via docker compose

+ build containers

```
docker compose build
```

+ make `.env` file, see `.envSAMPLE` for content
    
    + you should have `TASTYTRADE_CLIENT_SECRET` and `TASTYTRADE_REFRESH_TOKEN` after follwing above step.

    + `EXPECTED_HASH` is the hashed password to the website, use below to create password hash in python via bcrypt:

    ```
    bcrypt.hashpw("thisisyourpassword".encode('utf-8'), bcrypt.gensalt())
    ```

+ spin up all services, remember to specify the `.env` filepath.

```
docker compose --env-file .env up -d
```

#### via kube (ignore this, for personal use... )

For devops folks these are shitty notes, likely okay to follow. For non-devops folks, these will look like garbage.

+ compute resource setup notes see `.manifest-infra/README.md`

+ setup kube secrets for pulling from docker hub, and tasty api authentication, see `.manifest-back/README.md`

+ docker building in `.timescaledb/build_and_push.sh`, `flask/build_and_push.sh`.

+ volume and database setup notes see `.manifest-volume/README.md`, `.timescaledb/README.md`.

+ services setup notes see `.manifest-back/README.md`, `.manifest-front/README.md`.

+ cicd notes see `.github/workflows`

+ dns notes see `.cloudflared`


## additional comments

+ if one prefers to hoard data, adjust timescaldb retention_policy, see `.timescaledb/08-retention.sql`.
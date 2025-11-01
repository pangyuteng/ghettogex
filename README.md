
# hola ghettogex

Below are my deployment notes. for devops folks likely okay to follow. for non-devops folks - these will look like garbage.

+ compute resource setup notes see `.manifest-infra/README.md`

+ setup kube secrets for pulling from docker hub, and tasty api authentication, see `.manifest-back/README.md`

+ docker building in `.timescaledb/build_and_push.sh`, `flask/build_and_push.sh`.

+ volume and database setup notes see `.manifest-volume/README.md`, `.timescaledb/README.md`.

+ services setup notes see `.manifest-back/README.md`, `.manifest-front/README.md`.

+ cicd notes see `.github/workflows`

+ dns notes see `.cloudflared`



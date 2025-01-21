GREEN  := "\\u001b[32m"
RESET  := "\\u001b[0m\\n"
CHECK  := "\\xE2\\x9C\\x94"

set shell := ["bash", "-uc"]

set dotenv-required

default:
  just -l

venv:
  uv pip install -r ./pacifico/requirements.txt -r ./corona/requirements.txt -r ./modelo/requirements.txt

build opts="":
  docker compose build {{opts}}

dev opts="":
  docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d {{opts}}

down opts="":
  docker compose down {{opts}}

prod image version='latest':
  export VERSION={{version}} && docker compose pull
  export VERSION={{version}} && docker rollout {{image}}
  @printf " {{GREEN}}{{CHECK}} Images Started {{CHECK}} {{RESET}}"

rollback image:
  export VERSION=$(docker image ls --filter before=manifoldlabs/targon-{{image}}:latest --filter reference=manifoldlabs/targon-hub-{{image}} --format "{{{{.Tag}}" | head -n 1) && docker rollout {{image}}

k8s-up: k8s-create k8s-build k8s-load k8s-deploy

k8s-create:
  kind create cluster --config ./k8s-deployment/local-kind-config.yaml

k8s-build:
  docker buildx build -t manifoldlabs/targon-pacifico:dev -f Dockerfile.pacifico . --platform linux/amd64,linux/arm64
  docker buildx build -t manifoldlabs/targon-modelo:dev -f Dockerfile.modelo . --platform linux/amd64,linux/arm64
  docker buildx build -t manifoldlabs/targon-corona:dev -f Dockerfile.corona . --platform linux/amd64,linux/arm64

k8s-load:
  kind load docker-image manifoldlabs/targon-pacifico:dev \
    manifoldlabs/targon-modelo:dev \
    manifoldlabs/targon-corona:dev

k8s-deploy:
  envsubst < ./k8s-deployment/deployments.yaml | kubectl apply -f -

k8s-delete:
  kubectl delete -f ./k8s-deployment/deployments.yaml

k8s-down:
  kind delete cluster
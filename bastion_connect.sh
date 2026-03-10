#!/usr/bin/env bash
# connect-bastion-ssm.sh
# Connexion au bastion via AWS SSM avec installation auto du plugin si nécessaire
# Support du port forwarding automatique vers PostgreSQL

set -euo pipefail

# Defaults
REGION="eu-west-3"
AWS_PROFILE=""
AWS_ACCESS_KEY=""
AWS_SECRET_KEY=""
AWS_SESSION_TOKEN=""
INSTANCE_ID=""
INSTANCE_NAME=""
POSTGRES_HOST=""
POSTGRES_PORT="5432"
LOCAL_PORT="5432"
BACKGROUND_MODE=false
USE_CUSTOM_HOST=false
ASSUME_ROLE_ARN=""
EXTERNAL_ID=""

print_help() {
  cat <<EOF
Usage: $0 [options]

Options de connexion:
  --instance <instance-id>       : ID de l'instance EC2 (ex: i-0123456789abcdef)
  --name <Name-tag>              : Valeur du tag 'Name' pour retrouver l'instance
  --profile <aws-profile>        : Profil AWS à utiliser
  --access-key <AWS_ACCESS_KEY>  : Clé d'accès AWS
  --secret-key <AWS_SECRET_KEY>  : Clé secrète AWS
  --session-token <AWS_SESSION_TOKEN> : Token session (optionnel)
  --region <aws-region>          : Région AWS (default: ${REGION})

Options cross-account:
  --assume-role <role-arn>       : ARN du rôle à assumer pour l'accès cross-account
  --external-id <external-id>    : External ID pour l'assume role (sécurité)

Options de port forwarding PostgreSQL:
  --forward <postgres-hostname>  : Nom d'hôte du PostgreSQL pour le port forwarding
  --postgres-port <port>         : Port PostgreSQL distant (default: 5432)
  --local-port <port>            : Port local (default: 5432)
  --background                   : Lance le tunnel en arrière-plan
  --custom-host                  : Utilise le nom court extrait du hostname (ex: prod-rds-replica)

Autres:
  -h, --help                     : Affiche cette aide

Exemples:
  # Connexion interactive normale
  $0 --name my-bastion --profile prod

  # Port forwarding vers PostgreSQL
  $0 --name my-bastion --profile prod --forward my-postgres.cluster-xxx.eu-west-3.rds.amazonaws.com

  # Port forwarding avec nom extrait automatiquement
  $0 --name my-bastion --profile prod --forward prod-rds-replica.cto2gdmsi0x4.eu-west-3.rds.amazonaws.com --custom-host

  # Cross-account avec assume role
  $0 --name my-bastion --assume-role arn:aws:iam::COMPTE-PROD:role/BastionAccessRole --external-id bastion-access-2024 --forward prod-rds-replica.amazonaws.com
EOF
}

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance) INSTANCE_ID="$2"; shift 2;;
    --name) INSTANCE_NAME="$2"; shift 2;;
    --profile) AWS_PROFILE="$2"; shift 2;;
    --access-key) AWS_ACCESS_KEY="$2"; shift 2;;
    --secret-key) AWS_SECRET_KEY="$2"; shift 2;;
    --session-token) AWS_SESSION_TOKEN="$2"; shift 2;;
    --region) REGION="$2"; shift 2;;
    --forward) POSTGRES_HOST="$2"; shift 2;;
    --postgres-port) POSTGRES_PORT="$2"; shift 2;;
    --local-port) LOCAL_PORT="$2"; shift 2;;
    --background) BACKGROUND_MODE=true; shift 1;;
    --assume-role) ASSUME_ROLE_ARN="$2"; shift 2;;
    --external-id) EXTERNAL_ID="$2"; shift 2;;
    --custom-host) USE_CUSTOM_HOST=true; shift 1;;
    -h|--help) print_help; exit 0;;
    *) echo "Option inconnue: $1"; print_help; exit 1;;
  esac
done

# Vérification dépendances
if ! command -v aws >/dev/null 2>&1; then
  echo "❌ Erreur: aws CLI introuvable. Installe AWS CLI v2 d'abord."
  exit 2
fi

# Vérification/installation session-manager-plugin
if ! command -v session-manager-plugin >/dev/null 2>&1; then
  echo "⚠️  session-manager-plugin non trouvé. Installation en cours..."
  OS=$(uname -s | tr '[:upper:]' '[:lower:]')
  ARCH=$(uname -m)

  if [[ "$OS" == "linux" ]]; then
    if command -v yum >/dev/null 2>&1; then
      curl -s "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/linux_amd64/session-manager-plugin.rpm" -o "/tmp/session-manager-plugin.rpm"
      sudo yum install -y /tmp/session-manager-plugin.rpm
    elif command -v apt-get >/dev/null 2>&1; then
      curl -s "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" -o "/tmp/session-manager-plugin.deb"
      sudo dpkg -i /tmp/session-manager-plugin.deb || sudo apt-get install -f -y
    else
      echo "❌ Distribution Linux non supportée automatiquement. Installe manuellement le plugin."
      exit 3
    fi
  elif [[ "$OS" == "darwin" ]]; then
    if command -v brew >/dev/null 2>&1; then
      brew install session-manager-plugin
    else
      echo "❌ Homebrew introuvable. Installe manuellement le plugin sur macOS."
      exit 3
    fi
  else
    echo "❌ OS non supporté automatiquement ($OS). Installe manuellement le plugin."
    exit 3
  fi
  echo "✅ session-manager-plugin installé avec succès."
else
  echo "✅ session-manager-plugin déjà installé."
fi

# Export credentials si fournis
export AWS_REGION="$REGION"

# Gestion de l'assume role
if [[ -n "$ASSUME_ROLE_ARN" ]]; then
  echo "🔐 Assume role vers: $ASSUME_ROLE_ARN"
  
  # Construction de la commande assume-role
  ASSUME_CMD="aws sts assume-role --role-arn $ASSUME_ROLE_ARN --role-session-name bastion-session-$(date +%s)"
  
  if [[ -n "$EXTERNAL_ID" ]]; then
    ASSUME_CMD="$ASSUME_CMD --external-id $EXTERNAL_ID"
  fi
  
  if [[ -n "$AWS_PROFILE" ]]; then
    ASSUME_CMD="$ASSUME_CMD --profile $AWS_PROFILE"
  fi
  
  # Exécution de l'assume role
  echo "📋 Récupération des credentials temporaires..."
  ASSUME_RESULT=$(eval "$ASSUME_CMD" --output json)
  
  if [[ $? -eq 0 ]]; then
    # Extraction des credentials
    export AWS_ACCESS_KEY_ID=$(echo "$ASSUME_RESULT" | jq -r '.Credentials.AccessKeyId')
    export AWS_SECRET_ACCESS_KEY=$(echo "$ASSUME_RESULT" | jq -r '.Credentials.SecretAccessKey')
    export AWS_SESSION_TOKEN=$(echo "$ASSUME_RESULT" | jq -r '.Credentials.SessionToken')
    
    echo "✅ Assume role réussi. Credentials temporaires configurés."
    
    # Désactiver le profil AWS pour utiliser les variables d'environnement
    unset AWS_PROFILE
  else
    echo "❌ Erreur lors de l'assume role. Vérifiez vos permissions et le rôle."
    exit 4
  fi
else
  # Configuration normale des credentials
  [[ -n "$AWS_ACCESS_KEY" ]] && export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY"
  [[ -n "$AWS_SECRET_KEY" ]] && export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_KEY"
  [[ -n "$AWS_SESSION_TOKEN" ]] && export AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN"
  [[ -n "$AWS_PROFILE" ]] && export AWS_PROFILE="$AWS_PROFILE"
fi

# Résolution instance si --name utilisé
if [[ -z "$INSTANCE_ID" && -n "$INSTANCE_NAME" ]]; then
  echo "🔎 Recherche de l'instance avec Name tag = '$INSTANCE_NAME'..."
  INSTANCE_ID=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=${INSTANCE_NAME}" "Name=instance-state-name,Values=running" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text --region "$REGION")
  
  if [[ -z "$INSTANCE_ID" || "$INSTANCE_ID" == "None" ]]; then
    echo "❌ Erreur: aucune instance running trouvée avec Name='$INSTANCE_NAME' dans $REGION."
    exit 3
  fi
  echo "✅ Instance trouvée: $INSTANCE_ID"
fi

if [[ -z "$INSTANCE_ID" ]]; then
  echo "❌ Erreur: aucun instance-id fourni. Utilise --instance ou --name."
  exit 1
fi

# Gestion du port forwarding PostgreSQL
if [[ -n "$POSTGRES_HOST" ]]; then
  # Extraction du nom court pour l'alias
  CUSTOM_HOST_NAME=$(echo "$POSTGRES_HOST" | cut -d'.' -f1)
  
  # Vérification que le port local est libre
  if command -v netstat >/dev/null 2>&1 && netstat -tuln 2>/dev/null | grep -q ":$LOCAL_PORT "; then
    echo "⚠️  Port local $LOCAL_PORT déjà en cours d'utilisation"
    echo "💡 Vous pouvez utiliser --local-port pour choisir un autre port"
    read -p "Continuer quand même ? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      exit 0
    fi
  fi
  
  # Nettoyage de l'alias host si mode interactif et custom host utilisé
  if [[ "$USE_CUSTOM_HOST" == true && "$BACKGROUND_MODE" == false ]]; then
    trap 'echo ""; echo "🧹 Nettoyage de l'\''alias host..."; sudo sed -i "/'$CUSTOM_HOST_NAME'/d" /etc/hosts 2>/dev/null || true; echo "✅ Alias '$CUSTOM_HOST_NAME' supprimé"' EXIT
  fi
  
  # Configuration de l'alias host si demandé
  if [[ "$USE_CUSTOM_HOST" == true ]]; then
    # Backup du fichier hosts
    sudo cp /etc/hosts /etc/hosts.backup.$(date +%s) 2>/dev/null || true
    
    # Suppression de l'ancienne entrée si elle existe
    sudo sed -i "/$CUSTOM_HOST_NAME/d" /etc/hosts 2>/dev/null || true
    
    # Ajout de la nouvelle entrée
    echo "127.0.0.1 $CUSTOM_HOST_NAME" | sudo tee -a /etc/hosts >/dev/null
    
    echo "📝 Alias créé: $CUSTOM_HOST_NAME -> 127.0.0.1"
    CONNECTION_HOST="$CUSTOM_HOST_NAME"
  else
    CONNECTION_HOST="localhost"
  fi
  
  echo "🚀 Démarrage du port forwarding PostgreSQL..."
  echo "🐘 Local: $CONNECTION_HOST:$LOCAL_PORT -> PostgreSQL: $POSTGRES_HOST:$POSTGRES_PORT"
  echo "💡 Connectez-vous avec: psql -h $CONNECTION_HOST -p $LOCAL_PORT -U username -d database"
  
  if [[ "$BACKGROUND_MODE" == true ]]; then
    echo "🔄 Mode arrière-plan activé"
    echo "📝 PID du tunnel sera affiché après connexion"
    echo "🛑 Pour arrêter: kill \$(pgrep -f \"start-session.*$INSTANCE_ID\")"
    if [[ "$USE_CUSTOM_HOST" == true ]]; then
      echo "🧹 N'oubliez pas de nettoyer /etc/hosts après usage"
    fi
    echo ""
  else
    echo "🔌 Pour arrêter, utilisez Ctrl+C"
    if [[ "$USE_CUSTOM_HOST" == true ]]; then
      echo "🧹 L'alias host sera nettoyé automatiquement"
    fi
    echo ""
  fi
  
  # Lancement du port forwarding
  if [[ "$BACKGROUND_MODE" == true ]]; then
    aws ssm start-session \
      --target "$INSTANCE_ID" \
      --document-name "AWS-StartPortForwardingSessionToRemoteHost" \
      --parameters "{\"host\":[\"$POSTGRES_HOST\"],\"portNumber\":[\"$POSTGRES_PORT\"],\"localPortNumber\":[\"$LOCAL_PORT\"]}" \
      --region "$REGION" &
    TUNNEL_PID=$!
    echo "✅ Tunnel PostgreSQL démarré en arrière-plan (PID: $TUNNEL_PID)"
    echo "🔗 Connexion disponible sur $CONNECTION_HOST:$LOCAL_PORT"
    echo "🛑 Pour arrêter: kill $TUNNEL_PID"
  else
    aws ssm start-session \
      --target "$INSTANCE_ID" \
      --document-name "AWS-StartPortForwardingSessionToRemoteHost" \
      --parameters "{\"host\":[\"$POSTGRES_HOST\"],\"portNumber\":[\"$POSTGRES_PORT\"],\"localPortNumber\":[\"$LOCAL_PORT\"]}" \
      --region "$REGION"
  fi
else
  # Connexion interactive normale
  echo "🚀 Connexion à $INSTANCE_ID via SSM (région $REGION)..."
  aws ssm start-session --target "$INSTANCE_ID" --region "$REGION"
fi
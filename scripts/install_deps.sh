#!/bin/bash
set -e

# Verifica se é root/sudo
if [ "$EUID" -ne 0 ]; then
  echo "Por favor, rode este target com privilégios administrativos (ex: sudo make init ou sudo ./scripts/install_deps.sh)"
  exit 1
fi

echo "Atualizando sistema e dependências..."
apt-get update -y
apt-get install -y curl wget apt-transport-https virtualbox virtualbox-ext-pack software-properties-common ca-certificates gnupg lsb-release

# Docker
if ! command -v docker &> /dev/null; then
    echo "Instalando Docker..."
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    rm get-docker.sh
    # Adiciona o usuário logado que chamou o sudo ao grupo docker
    if [ -n "$SUDO_USER" ]; then
        usermod -aG docker $SUDO_USER
    fi
else
    echo "Docker já instalado."
fi

# K3s (Kubernetes leve - já inclui o Kubectl)
if ! command -v k3s &> /dev/null; then
    echo "Instalando K3s..."
    curl -sfL https://get.k3s.io | sh -s - server
    
    # Prepara o KUBECONFIG para o usuário regular não precisar de sudo ao usar o kubectl
    if [ -n "$SUDO_USER" ]; then
        mkdir -p /home/$SUDO_USER/.kube
        cp /etc/rancher/k3s/k3s.yaml /home/$SUDO_USER/.kube/config
        chown $SUDO_USER:$SUDO_USER /home/$SUDO_USER/.kube/config
        chmod 600 /home/$SUDO_USER/.kube/config
    fi
else
    echo "K3s já instalado."
fi

echo "======================================================================"
echo "Instalação finalizada com sucesso!"
echo "Se esta é a sua primeira vez instalando o Docker, por favor DESLOGUE"
echo "E LOGUE NOVAMENTE no sistema, ou rode 'su - \$SUDO_USER' para"
echo "recarregar as permissões do grupo Docker."
echo "======================================================================"

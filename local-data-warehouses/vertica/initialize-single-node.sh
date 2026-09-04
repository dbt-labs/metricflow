#!/bin/bash
# Bootstraps a single-node Vertica database inside the opentext/vertica-k8s container.
#
# The dedicated Vertica Community Edition image (vertica-ce) was removed from Docker Hub in 2025, so this
# script performs the database bootstrap that the VerticaDB Kubernetes operator would normally do:
#   1. Generate the TLS certificates required by the node management agent (NMA) and the vcluster CLI.
#   2. Start the NMA.
#   3. Create a single-node database with vcluster (or restart it if the catalog already exists).
#   4. Create the application user and install the optional packages needed by the MetricFlow test suite
#      (approximate for APPROXIMATE_PERCENTILE, ComplexTypes for multi-row INSERT ... VALUES support).
#
# The container starts as root to fix directory ownership, then drops to the unprivileged daemon user.
set -e

DB_NAME=${VERTICA_DB_NAME:-metricflow}
APP_USER=${APP_DB_USER:-metricflow}
APP_PASSWORD=${APP_DB_PASSWORD:-metricflowing}
DATA_DIR=/data
CERT_DIR=/opt/vertica/config/https_certs

mkdir -p "$DATA_DIR" /vertica/tmp "$CERT_DIR"
chown -R daemon:daemon /opt/vertica/log /opt/vertica/config "$DATA_DIR" /vertica

exec runuser -u daemon -- bash -e <<EOSU
export DB_NAME="$DB_NAME" APP_USER="$APP_USER" APP_PASSWORD="$APP_PASSWORD" DATA_DIR="$DATA_DIR" CERT_DIR="$CERT_DIR"

cd "\$CERT_DIR"
if [ ! -f rootca.pem ]; then
    # Self-signed root CA plus one CA-signed certificate, used by the Vertica HTTPS service, the NMA
    # (server), and the vcluster CLI (client). vcluster looks for {os-user}.pem / {os-user}.key in this
    # directory.
    openssl req -x509 -newkey rsa:4096 -nodes -days 3650 -keyout rootca.key -out rootca.pem -subj "/CN=rootca"
    openssl req -newkey rsa:4096 -nodes -keyout node.key -out node.csr -subj "/CN=daemon"
    openssl x509 -req -in node.csr -CA rootca.pem -CAkey rootca.key -CAcreateserial -out node.pem -days 3650 \
        -extfile <(echo "subjectAltName=DNS:localhost,IP:127.0.0.1")
    cp node.pem nma_cert.pem
    cp node.key nma_key.pem
    cp node.pem daemon.pem
    cp node.key daemon.key

    # The Vertica server's HTTPS service reads its key and certificates inline from this file. Without
    # it, the node never reports as up during vcluster create_db.
    cat <<EOF > httpstls.json
{"name": "https", "mode": 2, "key": "\$(awk '{printf "%s\\\\n", \$0}' node.key)", "certificate": "\$(awk '{printf "%s\\\\n", \$0}' node.pem)", "chain_certs": [], "ca_certificates": ["\$(awk '{printf "%s\\\\n", \$0}' rootca.pem)"]}
EOF
fi

export NMA_ROOTCA_PATH="\$CERT_DIR/rootca.pem"
export NMA_CERT_PATH="\$CERT_DIR/nma_cert.pem"
export NMA_KEY_PATH="\$CERT_DIR/nma_key.pem"
/opt/vertica/bin/node_management_agent &

for i in \$(seq 1 30); do
    curl -sk https://127.0.0.1:5554/v1/health >/dev/null && break
    sleep 1
done

if [ ! -d "\$DATA_DIR/\$DB_NAME" ]; then
    /opt/vertica/bin/vcluster create_db --db-name "\$DB_NAME" --hosts 127.0.0.1 \
        --catalog-path "\$DATA_DIR" --data-path "\$DATA_DIR" --password "\$APP_PASSWORD"

    vsql -h 127.0.0.1 -d "\$DB_NAME" -U daemon -w "\$APP_PASSWORD" \
        -c "CREATE USER \$APP_USER IDENTIFIED BY '\$APP_PASSWORD'; GRANT PSEUDOSUPERUSER TO \$APP_USER; ALTER USER \$APP_USER DEFAULT ROLE PSEUDOSUPERUSER;"

    # The vertica-k8s image does not install these packages by default.
    vsql -h 127.0.0.1 -d "\$DB_NAME" -U daemon -w "\$APP_PASSWORD" -f /opt/vertica/packages/approximate/ddl/install.sql
    vsql -h 127.0.0.1 -d "\$DB_NAME" -U daemon -w "\$APP_PASSWORD" -f /opt/vertica/packages/ComplexTypes/ddl/install.sql
else
    /opt/vertica/bin/vcluster start_db --db-name "\$DB_NAME" --hosts 127.0.0.1 \
        --catalog-path "\$DATA_DIR" --password "\$APP_PASSWORD"
fi

echo "Vertica database '\$DB_NAME' is ready on port 5433."
wait
EOSU

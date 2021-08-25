ARG version
FROM postgres:${version}

# Use non-standard encoding for tests
RUN localedef -i cs_CZ -c -f ISO-8859-2 -A /usr/share/locale/locale.alias cs_CZ.ISO-8859-2
ENV LANG cs_CZ.ISO-8859-2

# Fix SSL configuration to be compatible with older servers
RUN \
    # https://wiki.debian.org/ContinuousIntegration/TriagingTips/openssl-1.1.1
    sed -i 's/CipherString\s*=.*/CipherString = DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf \
    # https://stackoverflow.com/questions/53058362/openssl-v1-1-1-ssl-choose-client-version-unsupported-protocol
    && sed -i 's/MinProtocol\s*=.*/MinProtocol = TLSv1/g' /etc/ssl/openssl.cnf

# Copy certificates, SSL must by enabled by args: "-c ssl=on -c ssl_ca_file=/ssl-cert/ca-cert.pem -c ssl_cert_file=/ssl-cert/server-cert.pem -c ssl_key_file=/ssl-cert/server-key.pem"
COPY certificates /ssl-cert
RUN chmod 700 -R /ssl-cert/
RUN chown postgres:postgres -R /ssl-cert/

# Force SSL mode, if used arg "-c hba_file=/etc/postgresql/pg_hba_ssl.conf"
COPY pg_hba_ssl.conf /etc/postgresql/pg_hba_ssl.conf

FROM gcr.io/cloud-spanner-emulator/devcontainer AS builder

# Inject corporate CA certificates via shared script
COPY corporate-ca.pem /tmp/corporate-ca.pem
COPY inject-certs.sh /tmp/inject-certs.sh
RUN chmod +x /tmp/inject-certs.sh && /tmp/inject-certs.sh /tmp/corporate-ca.pem
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Install GCC 13 (ZetaSQL needs C++20, spanner_pg needs C23 enum:int — GCC 13 supports both)
RUN apt-get update && \
    apt-get install -y --no-install-recommends software-properties-common && \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
    apt-get update && \
    apt-get install -y --no-install-recommends gcc-13 g++-13 && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 130 \
                        --slave /usr/bin/g++ g++ /usr/bin/g++-13 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .

# Configure Bazel with truststore + extended HTTP timeouts for corporate proxy
RUN echo 'startup --host_jvm_args=-Djavax.net.ssl.trustStore=/tmp/truststore.jks' >> .bazelrc && \
    echo 'startup --host_jvm_args=-Djavax.net.ssl.trustStorePassword=changeit' >> .bazelrc && \
    echo 'startup --host_jvm_args=-Dsun.net.client.defaultConnectTimeout=300000' >> .bazelrc && \
    echo 'startup --host_jvm_args=-Dsun.net.client.defaultReadTimeout=300000' >> .bazelrc && \
    echo 'common --experimental_scale_timeouts=10.0' >> .bazelrc

RUN bazel build //binaries:emulator_main //binaries:gateway_main \
      --verbose_failures \
      --experimental_scale_timeouts=10.0 \
    && mkdir -p /output \
    && cp bazel-bin/binaries/emulator_main /output/ \
    && cp bazel-bin/binaries/gateway_main /output/ \
    && chmod +x /output/*

# Final stage: minimal image with just the binaries
FROM gcr.io/distroless/cc-debian12
COPY --from=builder /output/ /output/
CMD ["echo", "Binaries at /output/"]

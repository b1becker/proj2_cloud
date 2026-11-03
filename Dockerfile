FROM docclabgroup/raft-container:latest

RUN rm -rf /usr/local/go

RUN wget https://go.dev/dl/go1.22.12.linux-arm64.tar.gz \
    && tar -C /usr/local -xzf go1.22.12.linux-arm64.tar.gz \
    && rm go1.22.12.linux-arm64.tar.gz

ENV PATH="/usr/local/go/bin:${PATH}"

WORKDIR /raft-container/autograder-source/raft

CMD ["bash", "-c", "cp -r /raft-container/raft-main/. submission/ && exec bash"]
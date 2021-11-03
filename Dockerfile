FROM udomiljubimca/base-image:1.0

ADD ./server.sh ./requirements.txt ./src /app/

RUN pip install --no-cache-dir -r /app/requirements.txt && \
    chown -R appuser:root /app && \
    chmod -R g=u /app

WORKDIR /app

USER appuser

EXPOSE 8080

CMD ["/bin/bash", "server.sh"]
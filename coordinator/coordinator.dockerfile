FROM python:3.11.6-slim

ARG protocol="COR"
ARG interval=5

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

USER universalis

COPY --chown=universalis:universalis coordinator/requirements.txt /var/local/universalis/
COPY --chown=universalis:universalis universalis-package /var/local/universalis-package/

ENV PATH="/usr/local/universalis/.local/bin:${PATH}"

RUN pip install --user -r /var/local/universalis/requirements.txt \
    && pip install --user ./var/local/universalis-package/

WORKDIR /usr/local/universalis

COPY --chown=universalis:universalis coordinator coordinator

COPY --chown=universalis:universalis coordinator/start-coordinator.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-coordinator.sh

ENV PYTHONPATH /usr/local/universalis
ENV PROTOCOL_VAR ${protocol}
ENV INTERVAL_VAR ${interval}

USER universalis
CMD /usr/local/bin/start-coordinator.sh ${PROTOCOL_VAR} ${INTERVAL_VAR}

EXPOSE 8888
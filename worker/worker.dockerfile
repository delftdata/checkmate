FROM python:3.11.6-slim

ARG failure

RUN groupadd universalis \
    && useradd -m -d /usr/local/universalis -g universalis universalis

USER universalis

COPY --chown=universalis:universalis worker/requirements.txt /var/local/universalis/
COPY --chown=universalis:universalis universalis-package /var/local/universalis-package/

ENV PATH="/usr/local/universalis/.local/bin:${PATH}"

RUN pip install --user -r /var/local/universalis/requirements.txt \
    && pip install --user ./var/local/universalis-package/

WORKDIR /usr/local/universalis

COPY --chown=universalis:universalis worker worker

COPY --chown=universalis:universalis worker/start-worker.sh /usr/local/bin/
RUN chmod a+x /usr/local/bin/start-worker.sh

ENV PYTHONPATH /usr/local/universalis
ENV FAILURE_VAR ${failure}

USER universalis

CMD /usr/local/bin/start-worker.sh ${FAILURE_VAR}

EXPOSE 8888
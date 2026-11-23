# Build with the repository root as context.
FROM python:3.11-slim@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535 AS dependencies
WORKDIR /opt/writer
COPY services/audit-table-writer/requirements.txt .
RUN python -m pip install --disable-pip-version-check --no-cache-dir -r requirements.txt

FROM dependencies
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PORT=8080
COPY services/audit-table-writer/*.py ./
USER 65532:65532
EXPOSE 8080
CMD ["python", "writer.py"]

FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 BIND_HOST=0.0.0.0 PORT=8080
WORKDIR /app
COPY requirements.lock /app/requirements.lock
RUN pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml /app/
COPY src /app/src
RUN pip install --no-cache-dir --no-deps . && useradd --uid 10001 --create-home reference && mkdir /state && chown reference:reference /state
USER reference
EXPOSE 8080
CMD ["python", "-m", "chert_reference_agent"]

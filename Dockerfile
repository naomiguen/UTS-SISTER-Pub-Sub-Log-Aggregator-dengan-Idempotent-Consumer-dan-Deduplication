FROM python:3.11-slim

WORKDIR /app

# Buat non-root user untuk keamanan 
RUN adduser --disabled-password --gecos '' appuser \
    && chown -R appuser:appuser /app

USER appuser

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code aplikasi
COPY src/ ./src/

# Port yang di-expose
EXPOSE 8080

# Jalankan aplikasi
CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
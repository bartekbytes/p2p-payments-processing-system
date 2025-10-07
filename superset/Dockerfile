FROM apache/superset:4.1.4

USER root

# For PostgreSQL connection
RUN pip install psycopg2-binary

ENV ADMIN_USERNAME=$ADMIN_USERNAME
ENV ADMIN_EMAIL=$ADMIN_EMAIL
ENV ADMIN_PASSWORD=$ADMIN_PASSWORD

# Copy all assets (connections, charts, dashboards, ...) into image
COPY assets/ /app/assets

# Copy logo - for a small branding :)
COPY pps_logo.png /app/superset/static/assets/images/pps_logo.png

# Copy initialization file
COPY superset_init.sh /superset-init.sh

# Copy config file
COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

USER superset

ENTRYPOINT [ "/superset-init.sh" ]
# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

# Airflow with Claude Code, so the repair DAG diagnoses with the same model
# and effort as the host demo. The token comes from .env at run time; the
# image never contains one.
FROM apache/airflow:3.3.2

# The native installer puts this pinned version in ~/.local/bin of the airflow
# user, which is the user tasks run as. An update in place would change the
# version underneath a running demo, so the updater is off.
RUN curl -fsSL https://claude.ai/install.sh | bash -s 2.1.280
ENV PATH="/home/airflow/.local/bin:${PATH}" \
    DISABLE_AUTOUPDATER=1

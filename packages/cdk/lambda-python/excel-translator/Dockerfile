FROM public.ecr.aws/lambda/python:3.12

WORKDIR ${LAMBDA_TASK_ROOT}

# Install uv for fast dependency management
RUN pip install uv

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies
RUN uv pip install --system -r pyproject.toml

# Copy all handler files
COPY handler.py ./
COPY prepare_handler.py ./
COPY translate_batch_handler.py ./
COPY merge_handler.py ./

# Default handler (can be overridden by CMD in CDK)
CMD ["handler.lambda_handler"]

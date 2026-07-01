FROM public.ecr.aws/lambda/nodejs:24

COPY . ${LAMBDA_TASK_ROOT}/

RUN npm install \
    && npm run build

CMD ["dist/task.handler"]

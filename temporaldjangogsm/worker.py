import signal
import sys
import importlib
from concurrent.futures import ThreadPoolExecutor

# from temporalio.runtime import Runtime, TelemetryConfig, PrometheusConfig
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from temporaldjangogsm.client import connect


async def start_worker(host, port, namespace, task_queue, options, graceful_shutdown_timeout,
                       activities_path, workflows_path,
                       server_root_ca_cert=None, client_cert=None, client_key=None):
    activities_module = importlib.import_module(activities_path)
    workflows_module = importlib.import_module(workflows_path)
    activities = activities_module.ACTIVITIES
    workflows = workflows_module.WORKFLOWS

    # runtime = Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address="0.0.0.0:8596")))
    # TODO FIX runtime and NAMESPACE CONFIG
    client = await connect(host, port, namespace, server_root_ca_cert, client_cert, client_key)  # , runtime=runtime)

    if not options:
        options = dict()
    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities,
        workflow_runner=UnsandboxedWorkflowRunner(),
        activity_executor=ThreadPoolExecutor(),  # TODO fix this
        graceful_shutdown_timeout=graceful_shutdown_timeout,
        **options
    )

    # catch the TERM signal, and stop the worker gracefully
    # https://github.com/temporalio/sdk-python#worker-shutdown
    async def signal_handler(sig, frame):
        await worker.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)

    await worker.run()

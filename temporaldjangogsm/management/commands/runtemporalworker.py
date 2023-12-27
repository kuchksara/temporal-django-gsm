import asyncio
import logging
import ast
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from django.core.management.base import BaseCommand, CommandError
    from django.conf import settings

from temporaldjangogsm.worker import start_worker


class Command(BaseCommand):
    help = "Start Temporal Python Django-aware Worker"

    def add_arguments(self, parser):
        parser.add_argument("--temporal_host", default=settings.TEMPORAL_HOST, help="Hostname for Temporal Scheduler")
        parser.add_argument("--temporal_port", default=None, help="Port for Temporal Scheduler")
        parser.add_argument("--namespace", default=settings.TEMPORAL_NAMESPACE, help="Namespace to connect to")
        parser.add_argument("--task-queue", help="Task queue to service")
        parser.add_argument(
            "--server-root-ca-cert", default=settings.TEMPORAL_CLIENT_ROOT_CA, help="Optional root server CA cert"
        )
        parser.add_argument("--client-cert", default=settings.TEMPORAL_CLIENT_CERT, help="Optional client cert")
        parser.add_argument("--client-key", default=settings.TEMPORAL_CLIENT_KEY, help="Optional client key")
        parser.add_argument(
            '--extra-config',
            nargs='?',
            type=str,
            default=None
        )
        parser.add_argument("--graceful-shutdown-timeout", default=timedelta(minutes=5),
                            help="Amount of time after shutdown is called"
                                 "that activities are given to complete before their tasks are"
                                 "cancelled.")
        parser.add_argument("--activities_path", default='workflow', help="the path for activities")
        parser.add_argument("--workflows_path", default='workflow', help="the path for workflows")

    def handle(self, *args, **options):
        temporal_host = options.get("temporal_host")
        # TODO handle the situation that port does not needed
        temporal_port = options.get("temporal_port")
        namespace = options.get("namespace")
        task_queue = options.get("task_queue")
        server_root_ca_cert = options.get("server_root_ca_cert", None)
        client_cert = options.get("client_cert", None)
        client_key = options.get("client_key", None)
        graceful_shutdown_timeout = options.get("graceful_shutdown_timeout")
        activities_path = options.get("activities_path")
        workflows_path = options.get("workflows_path")
        extra_config = dict()
        if options['extra_config']:
            try:
                extra_config = ast.literal_eval(options['extra_config'])

            except SyntaxError:
                input_dict = options['extra_config']
                raise CommandError(f'Invalid dict input: {input_dict}')

        if options["client_key"]:
            options["client_key"] = "--SECRET--"

        logging.info(f"Starting Temporal Worker with options: {options}")
        asyncio.run(
            start_worker(
                temporal_host,
                temporal_port,
                namespace=namespace,
                task_queue=task_queue,
                server_root_ca_cert=server_root_ca_cert,
                client_cert=client_cert,
                client_key=client_key,
                graceful_shutdown_timeout=graceful_shutdown_timeout,
                activities_path=activities_path,
                workflows_path=workflows_path,
                options=extra_config,
            )
        )

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""Run L3/L4/L5 experiments against a persistent Online Boutique cluster."""

import argparse
import json
import os
import sys
import time
from datetime import datetime

from .agent import ClusterManager, ServiceMaintainer
from .module import (
    EnvironmentManagerFactory,
    Logger,
    ManagerConsumer,
    ManifestChaosInjector,
    MessageCollector,
    RabbitMQ,
    ServiceMaintainerConsumer,
    TrafficLoader,
    load_config,
    load_yaml,
)
from .module.system_state_collector import (
    ONLINE_BOUTIQUE_SERVICES,
    SystemStateCollector,
)


logger = Logger(__file__, 'INFO')
global_config = load_config()


def parse_args():
    parser = argparse.ArgumentParser(
        description='Run a high-level L3/L4/L5 experiment on persistent Online Boutique.'
    )
    parser.add_argument(
        '--instance',
        required=True,
        help='Dataset instance supplying workload/task metadata. Its legacy Sock Shop chaos is ignored.',
    )
    parser.add_argument(
        '--components',
        default=None,
        help='Deprecated: all ten Online Boutique ServiceMaintainers are always created.',
    )
    parser.add_argument('--timeout', type=int, default=900)
    parser.add_argument('--cache_seed', type=int, default=42)
    parser.add_argument(
        '--chaos-manifest',
        action='append',
        dest='chaos_manifests',
        help='Chaos Mesh CR to apply. Repeat for multiple CRs; defaults to both configured CRs.',
    )
    parser.add_argument(
        '--no-chaos',
        action='store_true',
        help='Run without applying Chaos Mesh CRs (useful for a baseline run).',
    )
    return parser.parse_args()


def main(args: argparse.Namespace):
    instance_path = os.path.join(
        global_config['dataset']['path'], f'{args.instance}.yaml'
    )
    test_case = load_yaml(instance_path)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    result_dir = os.path.join(global_config['result_path'], timestamp)
    os.makedirs(result_dir, exist_ok=True)

    # Every run gets an isolated control-message database.  This prevents stale
    # messages from a prior experiment and removes the RabbitMQ prerequisite.
    os.environ['ACV_MESSAGE_DB'] = os.path.join(result_dir, 'message_bus.sqlite3')

    environment = EnvironmentManagerFactory.get_instance().get_environment(
        global_config['project']['deployment'],
        logger=logger,
    )
    traffic_loader = TrafficLoader(
        component='frontend',
        namespace=global_config['project']['namespace'],
        mode=test_case.get('workload', 'moderate'),
        logger=logger,
    )
    manifests = (
        []
        if args.no_chaos
        else (args.chaos_manifests or global_config['experiment']['chaos_manifests'])
    )
    maximum_fault_duration = global_config['experiment'].get(
        'chaos_max_duration_seconds', 1800
    )
    shutdown_budget = global_config.get('agent', {}).get(
        'graceful_shutdown_seconds', 360
    )
    if manifests and args.timeout + shutdown_budget > maximum_fault_duration:
        raise ValueError(
            f'--timeout plus graceful shutdown budget '
            f'({args.timeout}s + {shutdown_budget}s) exceeds the configured '
            f'Chaos CR duration ({maximum_fault_duration}s). Increase both CR '
            'durations and experiment.chaos_max_duration_seconds together.'
        )
    chaos_injector = ManifestChaosInjector(manifests=manifests, logger=logger)

    message_collector = None
    manager_consumer = None
    maintainer_consumers = []
    faults_started = False

    try:
        environment.setup()
        environment.check_pods_ready()

        # Verify both telemetry sources before injecting faults. Empty series
        # are valid, but transport/API/query errors would invalidate the run.
        telemetry = SystemStateCollector(
            namespace=global_config['project']['namespace']
        )
        preflight_state = telemetry.capture()
        with open(
            os.path.join(result_dir, 'observability_preflight.json'),
            'w',
            encoding='utf-8',
        ) as file:
            json.dump(preflight_state, file, ensure_ascii=False, indent=2)
        telemetry_errors = telemetry.errors(preflight_state)
        if telemetry_errors:
            raise RuntimeError(
                'Observability preflight failed before fault injection: '
                + '; '.join(telemetry_errors)
            )

        traffic_loader.start()

        # Faults are active before any agent receives the experiment task.
        chaos_injector.start_experiment()
        faults_started = True

        components = list(ONLINE_BOUTIQUE_SERVICES)
        if args.components:
            logger.warning(
                '--components is ignored: all ten Online Boutique '
                'ServiceMaintainers are required for unknown root causes.'
            )

        maintainer_config = load_config('service_maintainers.yaml')
        missing = [
            component
            for component in components
            if component not in maintainer_config[global_config['project']['name']]
        ]
        if missing:
            raise ValueError(f'Missing ServiceMaintainer configuration: {missing}')

        audit_path = os.path.join(
            result_dir,
            global_config['observability'].get(
                'audit_filename', 'agent_actions.jsonl'
            ),
        )
        manager = ClusterManager._init_from_config(
            cache_seed=args.cache_seed if args.cache_seed != -1 else None,
            components=components,
            audit_path=audit_path,
        )
        maintainers = [
            ServiceMaintainer._init_from_config(
                service_name=component,
                cache_seed=args.cache_seed if args.cache_seed != -1 else None,
                audit_path=audit_path,
            )
            for component in components
        ]

        manager_consumer = ManagerConsumer(
            agent=manager,
            log_file_path=os.path.join(result_dir, 'manager.md'),
        )
        maintainer_consumers = [
            ServiceMaintainerConsumer(
                agent=agent,
                log_file_path=os.path.join(result_dir, f'{agent.name}.md'),
            )
            for agent in maintainers
        ]
        message_collector = MessageCollector()
        message_collector.start()
        for consumer in maintainer_consumers:
            consumer.start()
        manager_consumer.start()

        logger.warning(f'Experiment output directory: {result_dir}')
        control_bus = RabbitMQ(
            **global_config['rabbitmq']['manager']['exchange']
        )
        for queue in global_config['rabbitmq']['manager']['queues']:
            control_bus.add_queue(**queue)
        task = (
            f'{global_config["heartbeat"]["group_task_prefix"]}'
            f'{global_config["heartbeat"]["task"]}'
        )
        control_bus.publish(task, routing_keys=['manager'])

        try:
            time.sleep(args.timeout)
        except KeyboardInterrupt:
            logger.warning('Experiment interrupted by user.')
    finally:
        active_exception = sys.exc_info()[1]
        logger.info('Stopping experiment-owned processes and recovering faults.')
        cleanup_errors = []
        if manager_consumer:
            try:
                manager_consumer.stop()
            except Exception as exc:
                cleanup_errors.append(f'manager consumer: {exc}')
        for consumer in maintainer_consumers:
            try:
                consumer.stop()
            except Exception as exc:
                cleanup_errors.append(f'{consumer.name} consumer: {exc}')
        if message_collector:
            try:
                message_collector.stop()
            except Exception as exc:
                cleanup_errors.append(f'message collector: {exc}')
        if faults_started:
            try:
                chaos_injector.delete_experiment()
            except Exception as exc:
                cleanup_errors.append(f'fault recovery: {exc}')
        try:
            traffic_loader.stop()
        except Exception as exc:
            cleanup_errors.append(str(exc))
        try:
            environment.teardown()
        except Exception as exc:
            cleanup_errors.append(str(exc))
        if cleanup_errors:
            cleanup_message = 'Experiment cleanup errors: ' + '; '.join(cleanup_errors)
            if active_exception is not None:
                logger.error(cleanup_message)
            else:
                raise RuntimeError(cleanup_message)


if __name__ == '__main__':
    main(parse_args())

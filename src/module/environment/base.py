# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import time
import subprocess
import json
from abc import ABC, abstractmethod
from typing import Union

from ..base import Base
from ..utils import load_config, load_yaml

global_config = load_config()

class EnvironmentManager(ABC, Base):
    """
    Abstract base class for managing deployment environments. Provides common methods for resource customization
    and pod readiness checks, with abstract methods for setup and teardown that must be implemented by subclasses.
    """

    def __init__(self, **kwargs) -> None:
        """
        Initialize the environment manager with project-specific configurations.
        """
        self.deployment: str = ""
        self.project = global_config['project']['name']
        self.project_path = global_config['project']['path']
        self.namespace = global_config['project']['namespace']
        self.unhealthy_pods: int = 0
        super().__init__(**kwargs)

    @abstractmethod
    def setup(self, config_fpath: Union[str, None]):
        """
        Abstract method to set up the environment.
        Must be implemented by subclasses.
        """
        raise NotImplementedError('Method setup() must be implemented.')

    @abstractmethod
    def teardown(self):
        """
        Abstract method to tear down the environment.
        Must be implemented by subclasses.
        """
        raise NotImplementedError('Method teardown() must be implemented.')

    def customize_resource(self, config_fpath: Union[str, None]):
        """
        Customize Kubernetes resources based on a configuration file.

        Parameters:
        - config_fpath (Union[str, None]): Path to the YAML configuration file specifying resource customizations.

        The configuration can include:
        - 'delete': List of deployments to delete.
        - 'create': List of deployment YAML file paths to apply.
        - 'modify': List of deployment modifications (add, delete, replace).
        """
        config = load_yaml(config_fpath)
        resource_config = config['environment']
        self.unhealthy_pods = resource_config.get('unhealthy_pods', 0)

        # Delete resources
        if 'delete' in resource_config:
            for deployment in resource_config['delete']:
                subprocess.run(
                    ['kubectl', 'delete', 'deployment', deployment, '-n', self.namespace],
                    check=True,
                )

        # Create resources
        if 'create' in resource_config:
            for deployment_fpath in resource_config['create']:
                subprocess.run(
                    ['kubectl', 'apply', '-f', deployment_fpath, '-n', self.namespace],
                    check=True,
                )

        # Modify resources
        if 'modify' in resource_config:
            for deployment_info in resource_config['modify']:
                deployment = deployment_info['deployment']

                # Delete paths
                if 'delete' in deployment_info:
                    for item in deployment_info['delete']:
                        command = [
                            'kubectl', 'patch', 'deployment', deployment, '-n',
                            self.namespace, '-p', f'{{"op": "remove", "path": "{item}"}}'
                        ]
                        subprocess.run(command, check=True)

                # Add new paths
                if 'create' in deployment_info:
                    for item in deployment_info['create']:
                        command = [
                            'kubectl', 'patch', 'deployment', deployment, '-n', self.namespace, '--type=json',
                            '-p', f'[{{"op": "add", "path": "{item["path"]}", "value": "{item["value"]}"}}]'
                        ]
                        subprocess.run(command, check=True)

                # Modify existing paths
                if 'modify' in deployment_info:
                    for item in deployment_info['modify']:
                        command = [
                            'kubectl', 'patch', 'deployment', deployment, '-n', self.namespace, '--type=json',
                            '-p', f'[{{"op": "replace", "path": "{item["path"]}", "value": "{item["value"]}"}}]'
                        ]
                        subprocess.run(command, check=True)

    def check_pods_ready(self, interval: int = 15, timeout: int = 300):
        """
        Check if all pods in the namespace are ready.

        Parameters:
        - interval (int): Interval in seconds between readiness checks (default: 15).
        """
        self.info(f'Checking pods in persistent namespace {self.namespace}...')
        deadline = time.monotonic() + timeout
        required_deployments = (
            global_config.get('heartbeat', {}).get('components', [])
            if global_config.get('project', {}).get('reuse_existing', False)
            else []
        )
        while time.monotonic() < deadline:
            if required_deployments:
                result = subprocess.run(
                    ['kubectl', 'get', 'deployments', '-n', self.namespace, '-o', 'json'],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    raise RuntimeError(
                        f'Unable to inspect namespace {self.namespace}: '
                        f'{result.stderr.strip()}'
                    )
                deployments = {
                    item['metadata']['name']: item
                    for item in json.loads(result.stdout).get('items', [])
                }
                missing = [
                    name for name in required_deployments
                    if name not in deployments
                ]
                if missing:
                    raise RuntimeError(
                        'Persistent Online Boutique is missing deployments: '
                        + ', '.join(missing)
                    )
                ready_count = sum(
                    deployment.get('status', {}).get('availableReplicas', 0)
                    >= deployment.get('spec', {}).get('replicas', 1)
                    for name, deployment in deployments.items()
                    if name in required_deployments
                )
                total_count = len(required_deployments)
                self.info(f'Deployments Ready: {ready_count}/{total_count}')
                if ready_count == total_count:
                    self.info('All ten Online Boutique deployments are ready.')
                    return
                time.sleep(interval)
                continue

            result = subprocess.run(
                ['kubectl', 'get', 'pods', '-n', self.namespace, '-o', 'json'],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f'Unable to inspect namespace {self.namespace}: {result.stderr.strip()}'
                )
            items = json.loads(result.stdout).get('items', [])
            if not items:
                raise RuntimeError(
                    f'No pods found in persistent namespace {self.namespace}.'
                )
            ready_count = sum(
                any(
                    condition.get('type') == 'Ready'
                    and condition.get('status') == 'True'
                    for condition in pod.get('status', {}).get('conditions', [])
                )
                for pod in items
            )
            total_count = len(items)
            self.info(f'Pods Ready: {ready_count}/{total_count}')
            if ready_count + self.unhealthy_pods >= total_count:
                self.info('All expected pods are ready.')
                return
            time.sleep(interval)
        raise TimeoutError(
            f'Pods in namespace {self.namespace} did not become ready within {timeout}s.'
        )

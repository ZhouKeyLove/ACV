# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
from typing import Literal

def query_prometheus(promQL: str, **kwargs) -> list:
    """
    This function is used to query prometheus with the given promQL.
    - param promQL: str, the promQL to be executed
    - param kwargs: dict, parameters to be passed to the query, must contain one of the following: (start_time, end_time), duration
    
    return: list, result of the query
    
    Online Boutique metrics available in the supplied KPI & Resources dashboard:
    1. traces_spanmetrics_calls_total: request rate and error rate.
    2. traces_spanmetrics_duration_milliseconds_{sum,count,bucket}: mean and
       percentile latency. Important labels include service_name, span_kind,
       and status_code.
    3. container_cpu_usage_seconds_total,
       container_cpu_cfs_throttled_periods_total, and
       kube_pod_container_resource_limits for CPU.
    4. container_memory_working_set_bytes and container_memory_rss for memory.
    5. container_network_{receive,transmit}_bytes_total and
       container_fs_{reads,writes}_bytes_total for I/O.
    Kubernetes resource labels include namespace and pod.

    Note: ALWAYS call print() to report the result so that planner can get the result.

    Example: 
    >>> from src.agent.tool_functions_for_maintainer import query_prometheus
    >>> promQL = 'sum(rate(traces_spanmetrics_calls_total{service_name="checkoutservice",span_kind="SPAN_KIND_SERVER"}[2m]))'
    >>> result = query_prometheus(promQL=promQL, duration='2m', step='1m')
    >>> print(result) # output the result so that planner can get it.
    [['2024-06-20 02:17:20', 0.0], ['2024-06-20 02:18:20', 0.0], ['2024-06-20 02:19:20', 0.0]]
    """
    from src.module.prometheus_client import PrometheusClient
    prometheus_client = PrometheusClient()
    result: list[list[str, int]] = prometheus_client.query_range(promQL, **kwargs)
    return result


def query_jaeger_dependency_trace() -> dict:
    """
    Read Jaeger's service dependency graph for the configured lookback window.
    The result is filtered to edges involving the ten Online Boutique services.

    Note: ALWAYS call print() so the planner can read the result.

    Example:
    >>> from src.agent.tool_functions_for_maintainer import query_jaeger_dependency_trace
    >>> print(query_jaeger_dependency_trace())
    """
    from src.module.system_state_collector import SystemStateCollector

    return SystemStateCollector(namespace='onlineboutique').query_dependency_trace()

def report_result(component: str, message: str, message_type: Literal['ISSUE', 'RESPONSE']) -> str:
    """
    This function can help you send a message to the manager.
    - param component: str, the component name
    - param message: str, the message to be reported
    - param type: str, the type of the message, use 'ISSUE' for HEARTBEAT and 'RESPONSE' for TASK

    return: str, the result of the operation

    Note: ALWAYS call print() to report the result so that planner can get the result.

    Example:
    >>> from src.agent.tool_functions_for_maintainer import report_result
    >>> component = 'checkoutservice'
    >>> message = 'The task is completed.'
    >>> message_type = 'RESPONSE'
    >>> result = report_result(component=component, message=messages, message_type=message_type)
    >>> print(result) # output the result so that planner can get it.
    Message sent to manager.
    """
    from src.module import RabbitMQ, load_config

    global_config = load_config()

    queues = global_config['rabbitmq']['message_collector']['queues']
    rabbitmq = RabbitMQ(**global_config['rabbitmq']['message_collector']['exchange'])
    for queue in queues:
            rabbitmq.add_queue(**queue)

    if message_type == 'ISSUE':
        message = f'ISSUE from component {component}: \n {message}'
    elif message_type == 'RESPONSE':
        message = f'RESPONSE from component {component}: \n {message}'
    else:
        raise ValueError('Invalid message type.')

    rabbitmq.publish(
        message=message,
        routing_keys=['collector'],
        headers={'sender': component}
    )
        
    return 'Message sent to manager.'

# use this list to store all the functions, do not change the name
functions = [query_prometheus, query_jaeger_dependency_trace, report_result]

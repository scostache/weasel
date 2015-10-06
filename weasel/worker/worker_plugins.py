from weasel.worker.schedulers.static_node_scheduler import StaticNodeScheduler
from weasel.worker.schedulers.dynamic4_node_scheduler import Dynamic4NodeScheduler

schedulers_dict = {
    'static': StaticNodeScheduler,
    'dynamic4': Dynamic4NodeScheduler
}

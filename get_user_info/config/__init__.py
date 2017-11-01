from pkg_resources import resource_filename
from ti_config import bootstrap
from ti_daf import SqlTemplate

def ti_config_home():
    """
    获得本项目的配置目录
    :return:
    """
    return resource_filename(__name__, '')


def init_app():
    """
    初始化应用程序
    :return:
    """
    bootstrap.init_ti_srv_cfg('get-user-info', ti_config_home=ti_config_home())


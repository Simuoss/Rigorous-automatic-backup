from loguru import logger
import time
import os
import yaml
from auto_backup import CONFIG_FILE

this_time = time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime())
log_file_path = os.path.join('./logs', f'{this_time}.log')

log_level = 'INFO'
# 读取配置文件
config = {}
with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

if 'log_level' in config['common'].keys():
    log_level = config['common']['log_level']

# 配置 Loguru 记录器
logger.add(log_file_path, level = log_level, format='<green>[{time:YYYY-MM-DD HH:mm:ss}]</green> <level>{level}</level> - <cyan>{message}</cyan>')
logger.info(f'日志级别：{log_level}')
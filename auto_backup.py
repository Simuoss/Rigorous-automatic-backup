import os
import yaml
import time
from datetime import datetime
from logger_config import logger
from back_up import BackupTask

CONFIG_FILE = 'config.yaml'
EXECEPTION_NOTIFICATION_PATH = ''
LOG_LEVEL = ''

GLOBAL_DESTINATION = ''
GLOBAL_FREQUENCY = ''
GLOBAL_METHOD = ''
GLOBAL_PREDEFINE_PATTERNS = ''
WAKEUP_FREQUENCY = 7200
ZIP_CHUNK_SIZE = 1024 * 1024 * 500  # 每次读取的块大小，500MB


freq_dict = {
        'daily': 86400,
        'weekly': 604800,
        'monthly': 2592000,
        'yearly': 31536000,
    }

# 注释字典
comments = {
    'common': {
        'global_destination': '全局目标路径',
        'global_frequency': '全局备份频率：daily（每天）, weekly（每周）, monthly（每月）等，也可以是秒数',
        'global_method': '全局备份方法：copy（复制）, zip（压缩）等',
        'global_predefine_patterns': '全局预定义备份模式，可选值：all（目录下除了排除项以外的所有文件）, none（不备份）',
        'wakeup_frequency': '唤醒时间，单位秒，默认2小时',
        'zip_chunk_size': '每次读取的块大小，单位字节，默认500MB',
        'log_level': '日志等级，可选值：DEBUG, INFO, WARNING, ERROR, CRITICAL',
        'exception_notification_path' : '（非必须）报错告知目录，如果出现error，会把当前日志复制一份到这里',

    },
    'example': {
        'source_directory': '源目录【必选】',
        'destination_directory': '目标目录【可选】',
        'backup_method': '备份方法：copy（复制）, zip（压缩）【可选】',
        'backup_frequency': '备份频率：daily（每天）, weekly（每周）, monthly（每月），yearly（每年）等，也可以是秒数【可选】',
        'predefine_patterns': '预定义备份模式，可选值：all（目录下除了排除项以外的所有文件）, none（不备份）【可选】',
        'file_name': '重命名备份文件名，不包含后缀【可选】',
        'exclude_path_list': '文件夹排除项列表，相对路径',
        'exclude_file_list': '排除文件列表（也会匹配文件夹），根据正则表达式匹配',
        'last_backup_time': '上次备份时间，不要修改',
        'fail_count': '备份失败次数，超过五次之后就不尝试备份了，不要修改'
    }
}

def create_default_config():
    config = {
        'common': {
            'global_destination': '/path/to/global/destination',
            'global_frequency': 'daily',
            'global_method': 'copy',
            'global_predefine_patterns': 'all',
            'wakeup_frequency': 7200,
            'zip_chunk_size': 1024 * 1024 * 500,
            'log_level': 'INFO',
            'exception_notification_path' : '',
        },
        'example': {
            'source_directory': '/path/to/source',
            'destination_directory': '/path/to/destination',
            'backup_method': 'copy',
            'backup_frequency': 'daily',
            'predefine_patterns': 'all',
            'file_name': 'newname',
            # 排除项，相对路径
            'exclude_path_list': [
                '/relative/path/to/exclude',
                '/relative/path/to/exclude2',
            ],
            # 排除文件，根据正则表达式匹配，可选
            'exclude_file_list': [
                '.*\.log$',
                '.*\.txt$',
            ],
            'last_backup_time': 'current_time',
            'fail_count': 0
        }
    }

    save_config(config, comments)






# 检查配置文件是否符合规范
def check_config(config):
    try:
        # 检查全局配置
        if 'common' not in config.keys():
            logger.error('配置文件中缺少common配置项！请检查配置文件，或删除当前配置文件后，重新运行本程序以重置配置文件')
            raise Exception('配置文件中缺少common配置项！请检查配置文件，或删除当前配置文件后，重新运行本程序以重置配置文件')
        if 'global_destination' not in config['common'].keys():
            logger.error('配置文件中缺少common.global_destination配置项，将无法确定保存路径。请检查配置文件！')
            raise Exception('配置文件中缺少common.global_destination配置项，将无法确定保存路径。请检查配置文件！')
        if 'global_frequency' not in config['common'].keys():
            logger.error('配置文件中缺少common.global_frequency配置项，将无法确定备份频率。请检查配置文件！')
            logger.warning('备份频率可选值：daily（每天）, weekly（每周）, monthly（每月）等，也可以是秒数')
            raise Exception('配置文件中缺少common.global_frequency配置项，将无法确定备份频率。请检查配置文件！')
        if 'wakeup_frequency' not in config['common'].keys():
            logger.error('配置文件中缺少common.wakeup_frequency配置项，将无法确定唤醒频率。请检查配置文件！')
            raise Exception('配置文件中缺少common.wakeup_frequency配置项，将无法确定唤醒频率。请检查配置文件！')
        if 'global_method' not in config['common'].keys():
            logger.warning('配置文件中缺少common.global_method配置项，将使用默认值copy。备份方法可选值：copy（复制）, zip（压缩）等')
            config['common']['global_method'] = 'copy'
            save_config(config)
        if 'global_predefine_patterns' not in config['common'].keys():
            logger.warning('配置文件中缺少common.global_predefine_patterns配置项，将使用默认值all。预定义备份模式可选值：all（所有）, none（无）')
            config['common']['global_predefine_patterns'] = 'all'
            save_config(config)
        if 'zip_chunk_size' not in config['common'].keys():
            logger.warning('配置文件中缺少common.zip_chunk_size配置项，将使用默认值500MB。')
            config['common']['zip_chunk_size'] = 1024 * 1024 * 500
            save_config(config)

        
        # 如果全局备份频率不属于预定义的频率，或不是一个大于wakeup_frequency的数字，则抛出异常
        if not isinstance(config['common']['global_frequency'], int):
            if config['common']['global_frequency'] not in freq_dict.keys():
                logger.error('配置文件中common.global_frequency配置项的值不符合规范，应为daily（每天）, weekly（每周）, monthly（每月）等预定义的频率，或一个大于wakeup_frequency的数字')
                raise Exception('配置文件中common.global_frequency配置项的值不符合规范，应为daily（每天）, weekly（每周）, monthly（每月）等预定义的频率，或一个大于wakeup_frequency的数字')
        else:
            if config['common']['global_frequency'] < config['common']['wakeup_frequency']:
                logger.error('配置文件中common.global_frequency配置项的值不符合规范，应大于wakeup_frequency')
                raise Exception('配置文件中common.global_frequency配置项的值不符合规范，应大于wakeup_frequency')
        
        # 如果全局备份方法不属于预定义的方法，则抛出异常
        if config['common']['global_method'] not in ['copy', 'zip']:
            logger.error('配置文件中common.global_method配置项的值不符合规范，应为copy（复制）, zip（压缩）等预定义的方法')
            raise Exception('配置文件中common.global_method配置项的值不符合规范，应为copy（复制）, zip（压缩）等预定义的方法')
        
        # 如果全局预定义备份模式不属于预定义的模式，则抛出异常
        if config['common']['global_predefine_patterns'] not in ['all', 'none', 'server_world_only', 'mcdr_server_world_only']:
            logger.error('配置文件中common.global_predefine_patterns配置项的值不符合规范，应为all（所有）, none（无）, server_world_only（仅服务器存档）, mcdr_server_world_only（仅MCDR服务器存档）等预定义的模式')
            raise Exception('配置文件中common.global_predefine_patterns配置项的值不符合规范，应为all（所有）, none（无）, server_world_only（仅服务器存档）, mcdr_server_world_only（仅MCDR服务器存档）等预定义的模式')
        

        # 检查各个备份项配置
        for section in config.keys():
            if section != 'common' and section != 'example':
                if 'source_directory' not in config[section].keys():
                    logger.error(f'配置文件中缺少{section}.source_directory配置项')
                    raise Exception(f'配置文件中缺少{section}.source_directory配置项')
    
    except Exception as e:
        if EXECEPTION_NOTIFICATION_PATH != '':
            # 把当前日志复制到EXECEPTION_NOTIFICATION_PATH/error.log，如果没有则创建
            os.makedirs(EXECEPTION_NOTIFICATION_PATH, exist_ok=True)
            with open(os.path.join(EXECEPTION_NOTIFICATION_PATH, 'error.log'), 'w', encoding='utf-8') as file:
                with open('log.log', 'r', encoding='utf-8') as log:
                    file.write(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] {str(e)}\n')


# 读取配置文件
def load_config(file_path):
    # 如果配置文件不存在，则创建默认配置文件
    if not os.path.exists(file_path):
        logger.warning(f'配置文件{file_path}不存在，将创建默认配置文件')
        create_default_config()
    # 读取配置文件
    with open(file_path, 'r',encoding='utf-8') as yamlfile:
        config = yaml.safe_load(yamlfile)
    # 检查配置文件是否符合规范
    check_config(config)

    return config

# 确认是否备份文件
def backup_confirm(config, section):
    # 如果是全局或示例配置，则跳过
    if section == 'common' or section == 'example':
        return False
    # 如果备份模式为none，则跳过
    if config[section].get('predefine_patterns',GLOBAL_PREDEFINE_PATTERNS) == 'none':
        return False
    # 如果上次备份时间为空，也就是没备份过
    if config[section].get('last_backup_time', '') == '':
        logger.warning(f'检测到{section}上次备份时间为空，可能没有备份过，将跳过本次备份并更新上次备份时间为当前时间')
        return True
    
    # 解析备份频率,对应一段time库里的时长
    last_backup_time = config[section].get('last_backup_time', '')
    backup_frequency = config[section].get('backup_frequency', GLOBAL_FREQUENCY)

    # 如果备份频率不是数字，则转换为数字
    if not isinstance(backup_frequency, int):
        backup_frequency = freq_dict.get(backup_frequency, 86400)
    # 如果上次备份时间距离现在的时间小于备份频率，则跳过
    if (datetime.now() - datetime.strptime(last_backup_time, "%Y-%m-%d %H:%M:%S")).total_seconds() < backup_frequency - WAKEUP_FREQUENCY/4: 
        return False
    
    # 如果已经备份失败超过五次，强制启用两日一备份，避免因为备份间隔太快导致的硬盘爆满
    if config[section].get('fail_count', 0) >= 5:
        logger.warning(f'检测到{section}已经备份失败超过五次！')
        if backup_frequency < 2*86400:
            logger.warning(f'目前的备份频率过高，为防止因为备份间隔太快导致的硬盘爆满，将强制启用两日一备份！')
            if (datetime.now() - datetime.strptime(last_backup_time, "%Y-%m-%d %H:%M:%S")).total_seconds() < 2*86400 - WAKEUP_FREQUENCY/4: 
                # 直接更新备份时间
                config[section]['last_backup_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                return False
        

    logger.info(f'检测到{section}需要备份，上次备份时间为{last_backup_time}，距离现在的时间为{(datetime.now() - datetime.strptime(last_backup_time, "%Y-%m-%d %H:%M:%S")).seconds}秒，基本符合备份频率{backup_frequency}秒，将开始备份')
    return True


# 备份文件
def backup_files(config, section):
    # 如果上次备份时间为空，则跳过并且更新上次备份时间为今天
    if config[section].get('last_backup_time', '') == '':
        config[section]['last_backup_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return config
    
    source_dir = config[section].get('source_directory')
    destination_dir = config[section].get('destination_directory', GLOBAL_DESTINATION)
    file_name = config[section].get('file_name', os.path.basename(source_dir))

    backup_method = config[section].get('backup_method', GLOBAL_METHOD)
    predefine_patterns = config[section].get('predefine_patterns', GLOBAL_PREDEFINE_PATTERNS)

    exclude_path_list = config[section].get('exclude_path_list', [])
    exclude_file_list = config[section].get('exclude_file_list', [])

    task = BackupTask(source_dir, destination_dir, backup_method, predefine_patterns, exclude_path_list, exclude_file_list, file_name)
    if task.backup_files():#备份成功更新时间
        logger.info(f'更新上次备份时间为当前时间')
        config[section]['last_backup_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        if config[section].get('fail_count', 0) > 0:
            logger.warning(f'检测到{section}本次备份成功，清除失败次数')
        config[section]['fail_count'] = 0
    else:
        # 如果是第一次失败
        if config[section].get('fail_count', 0) == 0:
            logger.info(f'本次备份{section}失败，时间不更新！累计一次失败，如果连续五次失败，将进入风险备份模式')
        # 如果失败过小于五次
        elif config[section].get('fail_count', 0) < 5:
            logger.info(f'备份{section}再次失败，时间不更新！累计{config[section].get("fail_count", 0) + 1}次失败，如果连续五次失败，将进入风险备份模式')
        # 如果失败次数超过五次
        else:
            logger.warning(f'备份{section}累计失败五次以上，将进入风险备份模式，同时更新备份时间')

        config[section]['fail_count'] = config[section].get('fail_count', 0) + 1

    return config


def save_config(config, comments):
    with open(CONFIG_FILE, 'w',encoding='utf-8') as file:
        file.write('# common是全局配置，必须设置，如果后面的task缺少设置项，会默认全局配置\n')
        file.write('# example是示例配置，不会被解析\n')
        for section, settings in config.items():
            file.write(f'{section}:\n')
            for key, value in settings.items():
                comment = comments.get(section, {}).get(key)
                if comment:
                    file.write(f'  # {comment}\n')
                
                # 如果value是list，分行写入
                if isinstance(value, list):
                    file.write(f'  {key}:\n')
                    for item in value:
                        file.write(f'    - {item}\n')
                else:
                    file.write(f'  {key}: {value}\n')


# 运行备份
def run_backup():

    # 载入全局配置
    global GLOBAL_DESTINATION
    global GLOBAL_FREQUENCY
    global GLOBAL_METHOD
    global GLOBAL_PREDEFINE_PATTERNS
    global WAKEUP_FREQUENCY
    global ZIP_CHUNK_SIZE
    global EXECEPTION_NOTIFICATION_PATH
    global LOG_LEVEL
    global CONFIG_FILE

    logger.info('启动备份程序，开始读取配置文件：')
    # 读取配置文件
    config = load_config(CONFIG_FILE)



    GLOBAL_DESTINATION = config['common']['global_destination']
    GLOBAL_FREQUENCY = config['common']['global_frequency']
    GLOBAL_METHOD = config['common']['global_method']
    GLOBAL_PREDEFINE_PATTERNS = config['common']['global_predefine_patterns']
    WAKEUP_FREQUENCY = config['common']['wakeup_frequency']
    ZIP_CHUNK_SIZE = config['common']['zip_chunk_size']
    EXECEPTION_NOTIFICATION_PATH = config['common']['exception_notification_path']
    LOG_LEVEL = config['common'].get('log_level')


    logger.info(config)
    # 备份文件
    for section in config.keys():
        if backup_confirm(config, section):
            config = backup_files(config, section)
            logger.info('更新到配置文件')
            save_config(config)
    
    logger.info(f'执行了所有备份任务，{WAKEUP_FREQUENCY}秒后再见！')


if __name__ == '__main__':
    
    run_backup()
import re
import os
import shutil
import time
import zipfile
import concurrent.futures
from logger_config import logger
global ZIP_CHUNK_SIZE, EXECEPTION_NOTIFICATION_PATH

class BackupTask:
    def __init__(self, source_dir, destination_dir, backup_method, predefine_patterns, exclude_path_list, exclude_file_list, file_name):
        self.source_dir = source_dir
        self.destination_dir = destination_dir
        self.backup_method = backup_method
        self.predefine_patterns = predefine_patterns
        self.exclude_path_list = exclude_path_list
        self.exclude_file_list = exclude_file_list
        self.file_name = file_name

    def copy_file(self):
        try:
            logger.info(f'加载备份排除项列表：{self.exclude_path_list}')
            logger.info(f'加载备份排除文件列表：{self.exclude_file_list}')
            logger.info(f'开始备份：{self.source_dir} -> {self.destination_dir}，目标文件名：{self.file_name}')
            start_time = time.time()

            # 定义ignore函数用于过滤需要排除的文件或文件夹
            def ignore_func(dir, files):
                ignored = set()
                if self.exclude_path_list:
                    ignored.update(item for item in files if os.path.isdir(os.path.join(dir, item)) and item in self.exclude_path_list)
                if self.exclude_file_list:
                    pattern = re.compile('|'.join(self.exclude_file_list))
                    ignored.update(item for item in files if pattern.match(item))
                return ignored

            # 如果是文件，使用shutil.copy
            if os.path.isfile(self.source_dir):
                shutil.copy(self.source_dir, self.destination_dir + '/' + self.file_name + '_' + time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()) + os.path.splitext(self.source_dir)[1])
            # 如果是文件夹，使用shutil.copytree
            else:
                # 使用ignore参数排除指定的文件或文件夹
                shutil.copytree(self.source_dir, self.destination_dir + '/' + self.file_name + '_' + time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()), ignore=ignore_func, dirs_exist_ok=True)
            
            end_time = time.time()
            logger.info(f"备份完成：{self.source_dir} -> {self.destination_dir}，耗时：{end_time - start_time}秒")
        except Exception as e:
            logger.error(f"备份失败：{self.source_dir} -> {self.destination_dir} 错误信息：{str(e)}")
            return False
        return True

    def read_in_chunks(self,file_path, chunk_size):
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def zip_file(self):
        try:
            logger.info(f'加载备份排除项列表：{self.exclude_path_list}')
            logger.info(f'加载备份排除文件列表：{self.exclude_file_list}')
            logger.info(f'开始备份：{self.source_dir} -> {self.destination_dir}，目标文件名：{self.file_name}.zip')
            logger.info(f'压缩过程无需写入硬盘，无需担心硬盘写入寿命。内存最大占用量为500MB，可根据需要调整。')
            start_time = time.time()

            destination_zip = self.file_name + '_' + time.strftime("%Y-%m-%d_%H-%M-%S", time.localtime()) + '.zip'
            # 创建一个ZipFile对象，用于写入压缩文件
            with zipfile.ZipFile(self.destination_dir + '/' +destination_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zip_path_list = []  # 用于存储压缩文件的路径
                # 遍历文件夹下的所有文件，并根据排除项列表进行筛选备份
                for root, dirs, files in os.walk(self.source_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        rel_path = os.path.relpath(file_path, self.source_dir)
                        if (self.exclude_path_list and any(dir_name in rel_path for dir_name in self.exclude_path_list)) or \
                        (self.exclude_file_list and any(re.match(pattern, rel_path) for pattern in self.exclude_file_list)):
                            logger.info(f'排除文件：{file_path}')
                            continue  # 跳过符合排除条件的文件
                        
                        # 写入文件到压缩文件
                        zip_path_list.append((file_path, rel_path))
                with open(file_path, 'rb') as f:
                    global ZIP_CHUNK_SIZE
                    for file_path, rel_path in zip_path_list:
                        logger.info(f'正在压缩文件：{file_path}')
                        # 如果文件小于500M，直接压缩
                        if os.path.getsize(file_path) < ZIP_CHUNK_SIZE:
                            zipf.write(file_path, rel_path)
                        # 如果文件大于500M，分块压缩
                        else:
                            with open(file_path, 'rb') as f:
                                # 检查是否存在同名文件，如果存在则追加写入到末尾
                                if rel_path in zipf.namelist():
                                    with zipf.open(rel_path, mode='a') as existing_file:
                                        for chunk in self.read_in_chunks(file_path, ZIP_CHUNK_SIZE):
                                            existing_file.write(chunk)
                                else:
                                    # 创建新的同名文件并写入内容
                                    with zipf.open(rel_path, mode='w') as new_file:
                                        for chunk in self.read_in_chunks(file_path, ZIP_CHUNK_SIZE):
                                            new_file.write(chunk)

            end_time = time.time()           
            logger.info(f"备份完成：{self.source_dir} -> {self.destination_dir}，耗时：{end_time - start_time}秒")
        except Exception as e:
            logger.error(f"备份失败：{self.source_dir} -> {self.destination_dir} 错误信息：{str(e)}")
            if EXECEPTION_NOTIFICATION_PATH != '':
                # 把当前日志复制到EXECEPTION_NOTIFICATION_PATH/error.log，如果没有则创建
                os.makedirs(EXECEPTION_NOTIFICATION_PATH, exist_ok=True)
                with open(os.path.join(EXECEPTION_NOTIFICATION_PATH, 'error.log'), 'w', encoding='utf-8') as file:
                    with open('log.log', 'r', encoding='utf-8') as log:
                        file.write(f'[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] {str(e)}\n')
            return False
        return True

    def backup_files(self):
        os.makedirs(self.destination_dir, exist_ok=True)
        # 如果目标所在的硬盘的剩余空间<10GB，则跳过备份
        if shutil.disk_usage(self.destination_dir).free < 10 * 1024 * 1024 * 1024:
            logger.warning(f'目标所在的硬盘的剩余空间小于10GB，跳过备份')
            return False
        
        # 如果备份模式为all，则备份除过exclude list的所有文件
        if self.predefine_patterns == 'all':
            logger.info(f'备份模式：all，备份除过exclude list的所有文件')
            # 如果备份方法为copy，则复制文件
            if self.backup_method == 'copy':
                logger.info(f'备份方法：copy，直接复制文件')
                return self.copy_file() # 可以识别备份失败
            # 如果备份方法为zip，则压缩文件
            elif self.backup_method == 'zip':
                logger.info(f'备份方法：zip，压缩后复制文件')
                return self.zip_file()